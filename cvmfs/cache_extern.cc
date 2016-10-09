/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_extern.h"

#include <unistd.h>

#include <cassert>
#include <string>

#include "cache.pb.h"
#include "logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace {

int Ack2Errno(cvmfs::EnumStatus status_code) {
  switch (status_code) {
    case cvmfs::STATUS_OK:
      return 0;
    case cvmfs::STATUS_FORBIDDEN:
      return -EPERM;
    case cvmfs::STATUS_NOSPACE:
      return -ENOSPC;
    case cvmfs::STATUS_NOENTRY:
      return -ENOENT;
    case cvmfs::STATUS_MALFORMED:
      return -EINVAL;
    case cvmfs::STATUS_CORRUPTED:
      return -EIO;
    case cvmfs::STATUS_TIMEOUT:
      return -EIO;
    case cvmfs::STATUS_BADCOUNT:
      return -EINVAL;
    case cvmfs::STATUS_OUTOFBOUNDS:
      return -EINVAL;
    default:
      return -EIO;
  }
}

}  // anonymous namespace

const shash::Any ExternalCacheManager::kInvalidHandle;


int ExternalCacheManager::AbortTxn(void *txn) {
  return -1;
}


bool ExternalCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  assert(quota_mgr != NULL);
  quota_mgr_ = quota_mgr;
  LogCvmfs(kLogCache, kLogDebug, "set quota manager");
  return true;
}


void ExternalCacheManager::CallRemotely(ExternalCacheManager::RpcJob *rpc_job) {
  if (!spawned_) {
    transport_.SendFrame(rpc_job->frame_send());
    bool retval = transport_.RecvFrame(rpc_job->frame_recv());
    assert(retval);
  } else {
    // TODO
    abort();
  }
}


int ExternalCacheManager::ChangeRefcount(const shash::Any &id, int change_by) {
  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(id, &object_id);
  cvmfs::MsgRefcountReq msg_refcount;
  msg_refcount.set_session_id(session_id_);
  msg_refcount.set_req_id(NextRequestId());
  msg_refcount.set_allocated_object_id(&object_id);
  msg_refcount.set_change_by(change_by);
  RpcJob rpc_job(&msg_refcount);
  CallRemotely(&rpc_job);
  msg_refcount.release_object_id();

  cvmfs::MsgRefcountReply *msg_reply = rpc_job.msg_refcount_reply();
  return Ack2Errno(msg_reply->status());
}


int ExternalCacheManager::Close(int fd) {
  ReadOnlyHandle handle;
  {
    WriteLockGuard guard(rwlock_fd_table_);
    handle = fd_table_.GetHandle(fd);
    if (handle.id == kInvalidHandle)
      return -EBADF;
    int retval = fd_table_.CloseFd(fd);
    assert(retval == 0);
  }

  return ChangeRefcount(handle.id, -1);
}


int ExternalCacheManager::CommitTxn(void *txn) {
  return -1;
}


ExternalCacheManager *ExternalCacheManager::Create(
  int fd_connection,
  unsigned max_open_fds)
{
  UniquePtr<ExternalCacheManager> cache_mgr(
    new ExternalCacheManager(fd_connection, max_open_fds));
  assert(cache_mgr.IsValid());

  cvmfs::MsgHandshake msg_handshake;
  msg_handshake.set_protocol_version(kPbProtocolVersion);
  CacheTransport::Frame frame_send(&msg_handshake);
  cache_mgr->transport_.SendFrame(&frame_send);

  CacheTransport::Frame frame_recv;
  bool retval = cache_mgr->transport_.RecvFrame(&frame_recv);
  if (!retval)
    return NULL;
  google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();
  if (msg_typed->GetTypeName() != "cvmfs.MsgHandshakeAck")
    return NULL;
  cache_mgr->session_id_ =
    reinterpret_cast<cvmfs::MsgHandshakeAck *>(msg_typed)->session_id();
  return cache_mgr.Release();
}


void ExternalCacheManager::CtrlTxn(
  const ObjectInfo &object_info,
  const int flags,
  void *txn)
{
}


int ExternalCacheManager::DoOpen(const shash::Any &id) {
  int fd = -1;
  {
    WriteLockGuard guard(rwlock_fd_table_);
    fd = fd_table_.OpenFd(ReadOnlyHandle(id));
    if (fd < 0) {
      LogCvmfs(kLogCache, kLogDebug, "error while creating new fd",
               strerror(-fd));
      return fd;
    }
  }

  int status_refcnt = ChangeRefcount(id, 1);
  if (status_refcnt == 0)
    return fd;

  WriteLockGuard guard(rwlock_fd_table_);
  int retval = fd_table_.CloseFd(fd);
  assert(retval == 0);
  return status_refcnt;
}


int ExternalCacheManager::Dup(int fd) {
  shash::Any id = GetHandle(fd);
  if (id == kInvalidHandle)
    return -EBADF;
  return DoOpen(id);
}


shash::Any ExternalCacheManager::GetHandle(int fd) {
  ReadLockGuard guard(rwlock_fd_table_);
  ReadOnlyHandle handle = fd_table_.GetHandle(fd);
  return handle.id;
}


int64_t ExternalCacheManager::GetSize(int fd) {
  shash::Any id = GetHandle(fd);
  if (id == kInvalidHandle)
    return -EBADF;

  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(id, &object_id);
  cvmfs::MsgObjectInfoReq msg_info;
  msg_info.set_session_id(session_id_);
  msg_info.set_req_id(NextRequestId());
  msg_info.set_allocated_object_id(&object_id);
  msg_info.set_info_flags(cvmfs::OBJECT_INFO_SIZE);
  RpcJob rpc_job(&msg_info);
  CallRemotely(&rpc_job);
  msg_info.release_object_id();

  cvmfs::MsgObjectInfoReply *msg_reply = rpc_job.msg_object_info_reply();
  if (msg_reply->status() == cvmfs::STATUS_OK) {
    assert(msg_reply->has_size());
    return msg_reply->size();
  }
  return Ack2Errno(msg_reply->status());
}


ExternalCacheManager::ExternalCacheManager(
  int fd_connection,
  unsigned max_open_fds)
  : fd_table_(max_open_fds, ReadOnlyHandle())
  , transport_(fd_connection)
  , session_id_(-1)
  , spawned_(false)
{
  int retval = pthread_rwlock_init(&rwlock_fd_table_, NULL);
  assert(retval == 0);
  atomic_init64(&next_request_id_);
}


ExternalCacheManager::~ExternalCacheManager() {
  if (session_id_ >= 0) {
    cvmfs::MsgQuit msg_quit;
    msg_quit.set_session_id(session_id_);
    CacheTransport::Frame frame(&msg_quit);
    transport_.SendFrame(&frame);
  }
  close(transport_.fd_connection());
  pthread_rwlock_destroy(&rwlock_fd_table_);
}


int ExternalCacheManager::Open(const BlessedObject &object) {
  return DoOpen(object.id);
}


int ExternalCacheManager::OpenFromTxn(void *txn) {
  return -1;
}


int64_t ExternalCacheManager::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  shash::Any id = GetHandle(fd);
  if (id == kInvalidHandle)
    return -EBADF;

  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(id, &object_id);
  cvmfs::MsgReadReq msg_read;
  msg_read.set_session_id(session_id_);
  msg_read.set_req_id(NextRequestId());
  msg_read.set_allocated_object_id(&object_id);
  msg_read.set_offset(offset);
  msg_read.set_size(size);
  RpcJob rpc_job(&msg_read);
  rpc_job.set_attachment_recv(buf, size);
  CallRemotely(&rpc_job);
  msg_read.release_object_id();

  cvmfs::MsgReadReply *msg_reply = rpc_job.msg_read_reply();
  if (msg_reply->status() == cvmfs::STATUS_OK)
    return rpc_job.frame_recv()->att_size();

  return Ack2Errno(msg_reply->status());
}


int ExternalCacheManager::Readahead(int fd) {
  return -1;
}


int ExternalCacheManager::Reset(void *txn) {
  return -1;
}


uint16_t ExternalCacheManager::SizeOfTxn() {
  return 0;
}


int ExternalCacheManager::StartTxn(
  const shash::Any &id,
  uint64_t size,
  void *txn)
{
  return -1;
}


int64_t ExternalCacheManager::Write(const void *buf, uint64_t sz, void *txn) {
  return -1;
}
