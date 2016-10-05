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


void ExternalCacheManager::CallRemotely(
  google::protobuf::MessageLite *msg_req,
  google::protobuf::MessageLite *msg_reply)
{
  if (!spawned_) {
    transport_.SendMsg(msg_req);
    bool retval = transport_.RecvMsg(msg_reply);
    assert(retval);
  } else {
    // TODO
    abort();
  }
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

  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(handle.id, &object_id);
  cvmfs::MsgRefcountReq msg_refcount;
  msg_refcount.set_session_id(session_id_);
  msg_refcount.set_req_id(NextRequestId());
  msg_refcount.set_allocated_object_id(&object_id);
  msg_refcount.set_change_by(-1);
  cvmfs::MsgClientCall msg_req;
  msg_req.set_allocated_msg_refcount_req(&msg_refcount);
  cvmfs::MsgServerCall msg_reply;
  CallRemotely(&msg_req, &msg_reply);
  msg_req.release_msg_refcount_req();
  msg_refcount.release_object_id();

  assert(msg_reply.has_msg_refcount_reply());
  assert(msg_reply.msg_refcount_reply().req_id() == msg_refcount.req_id());
  return Ack2Errno(msg_reply.msg_refcount_reply().status());
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

  cvmfs::MsgClientCall msg_client_call;
  cvmfs::MsgHandshake msg_handshake;
  msg_handshake.set_protocol_version(kPbProtocolVersion);
  msg_client_call.set_allocated_msg_handshake(&msg_handshake);
  cache_mgr->transport_.SendMsg(&msg_client_call);
  msg_client_call.release_msg_handshake();

  cvmfs::MsgServerCall msg_ack;
  bool retval = cache_mgr->transport_.RecvMsg(&msg_ack);
  if (!retval || !msg_ack.has_msg_handshake_ack())
    return NULL;
  cache_mgr->session_id_ = msg_ack.msg_handshake_ack().session_id();
  return cache_mgr.Release();
}


void ExternalCacheManager::CtrlTxn(
  const ObjectInfo &object_info,
  const int flags,
  void *txn)
{
}


int ExternalCacheManager::Dup(int fd) {
  return -1;
}


int64_t ExternalCacheManager::GetSize(int fd) {
  return -1;
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
    cvmfs::MsgClientCall msg_client_call;
    msg_client_call.set_allocated_msg_quit(&msg_quit);
    transport_.SendMsg(&msg_client_call);
    msg_client_call.release_msg_quit();
  }
  close(transport_.fd_connection());
  pthread_rwlock_destroy(&rwlock_fd_table_);
}


int ExternalCacheManager::Open(const BlessedObject &object) {
  int fd = -1;
  {
    WriteLockGuard guard(rwlock_fd_table_);
    fd = fd_table_.OpenFd(ReadOnlyHandle(object.id));
    if (fd < 0) {
      LogCvmfs(kLogCache, kLogDebug, "error while opening %s: %s",
               object.id.ToString().c_str(), strerror(-fd));
      return fd;
    }
  }

  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(object.id, &object_id);
  cvmfs::MsgRefcountReq msg_refcount;
  msg_refcount.set_session_id(session_id_);
  msg_refcount.set_req_id(NextRequestId());
  msg_refcount.set_allocated_object_id(&object_id);
  msg_refcount.set_change_by(1);
  cvmfs::MsgClientCall msg_req;
  msg_req.set_allocated_msg_refcount_req(&msg_refcount);
  cvmfs::MsgServerCall msg_reply;
  CallRemotely(&msg_req, &msg_reply);
  msg_req.release_msg_refcount_req();
  msg_refcount.release_object_id();

  assert(msg_reply.has_msg_refcount_reply());
  assert(msg_reply.msg_refcount_reply().req_id() == msg_refcount.req_id());
  cvmfs::EnumStatus status_code = msg_reply.msg_refcount_reply().status();
  if (status_code == cvmfs::STATUS_OK)
    return fd;

  WriteLockGuard guard(rwlock_fd_table_);
  int retval = fd_table_.CloseFd(fd);
  assert(retval == 0);
  return Ack2Errno(status_code);
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
  return -1;
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
