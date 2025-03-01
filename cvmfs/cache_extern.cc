/**
 * This file is part of the CernVM File System.
 */

#include "cache_extern.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#ifdef __APPLE__
#include <cstdlib>
#endif
#include <cstring>
#include <map>
#include <new>
#include <set>
#include <string>

#include "cache.pb.h"
#include "crypto/hash.h"
#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#ifdef __APPLE__
#include "util/smalloc.h"
#endif
#include "util/string.h"

using namespace std;  // NOLINT

namespace {

int Ack2Errno(cvmfs::EnumStatus status_code) {
  switch (status_code) {
    case cvmfs::STATUS_OK:
      return 0;
    case cvmfs::STATUS_NOSUPPORT:
      return -EOPNOTSUPP;
    case cvmfs::STATUS_FORBIDDEN:
      return -EPERM;
    case cvmfs::STATUS_NOSPACE:
      return -ENOSPC;
    case cvmfs::STATUS_NOENTRY:
      return -ENOENT;
    case cvmfs::STATUS_MALFORMED:
      return -EINVAL;
    case cvmfs::STATUS_IOERR:
      return -EIO;
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
  int result = Reset(txn);
#ifdef __APPLE__
  free(reinterpret_cast<Transaction *>(txn)->buffer);
#endif
  return result;
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
    uint32_t save_att_size = rpc_job->frame_recv()->att_size();
    bool again;
    do {
      again = false;
      bool retval = transport_.RecvFrame(rpc_job->frame_recv());
      assert(retval);
      if (rpc_job->frame_recv()->IsMsgOutOfBand()) {
        google::protobuf::MessageLite *msg_typed =
          rpc_job->frame_recv()->GetMsgTyped();
        assert(msg_typed->GetTypeName() == "cvmfs.MsgDetach");
        quota_mgr_->BroadcastBackchannels("R");  //  release pinned catalogs
        rpc_job->frame_recv()->Reset(save_att_size);
        again = true;
      }
    } while (again);
  } else {
    Signal signal;
    {
      MutexLockGuard guard(lock_inflight_rpcs_);
      inflight_rpcs_.push_back(RpcInFlight(rpc_job, &signal));
    }
    {
      MutexLockGuard guard(lock_send_fd_);
      transport_.SendFrame(rpc_job->frame_send());
    }
    signal.Wait();
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
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  LogCvmfs(kLogCache, kLogDebug, "committing %s",
           transaction->id.ToString().c_str());
  int retval = Flush(true, transaction);
  if (retval != 0)
    return retval;

  int refcount = transaction->open_fds - 1;
  if (refcount != 0)
    return ChangeRefcount(transaction->id, refcount);
#ifdef __APPLE__
  free(transaction->buffer);
#endif
  return 0;
}


int ExternalCacheManager::ConnectLocator(
  const std::string &locator, bool print_error)
{
  vector<string> tokens = SplitString(locator, '=');
  int result = -1;
  if (tokens[0] == "unix") {
    result = ConnectSocket(tokens[1]);
  } else if (tokens[0] == "tcp") {
    vector<string> tcp_address = SplitString(tokens[1], ':');
    if (tcp_address.size() != 2)
      return -EINVAL;
    result = ConnectTcpEndpoint(tcp_address[0], String2Uint64(tcp_address[1]));
  } else {
    return -EINVAL;
  }
  if (result < 0) {
    if (print_error) {
      if (errno) {
        LogCvmfs(kLogCache, kLogDebug | kLogStderr,
                 "Failed to connect to socket: %s", strerror(errno));
      } else {
        LogCvmfs(kLogCache, kLogDebug | kLogStderr,
                 "Failed to connect to socket (unknown error)");
      }
    }
    return -EIO;
  }
  LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
           "connected to cache plugin at %s", locator.c_str());
  return result;
}


ExternalCacheManager *ExternalCacheManager::Create(
  int fd_connection,
  unsigned max_open_fds,
  const string &ident)
{
  UniquePtr<ExternalCacheManager> cache_mgr(
    new ExternalCacheManager(fd_connection, max_open_fds));
  assert(cache_mgr.IsValid());

  cvmfs::MsgHandshake msg_handshake;
  msg_handshake.set_protocol_version(kPbProtocolVersion);
  msg_handshake.set_name(ident);
  CacheTransport::Frame frame_send(&msg_handshake);
  cache_mgr->transport_.SendFrame(&frame_send);

  CacheTransport::Frame frame_recv;
  bool retval = cache_mgr->transport_.RecvFrame(&frame_recv);
  if (!retval)
    return NULL;
  google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();
  if (msg_typed->GetTypeName() != "cvmfs.MsgHandshakeAck")
    return NULL;
  cvmfs::MsgHandshakeAck *msg_ack =
    reinterpret_cast<cvmfs::MsgHandshakeAck *>(msg_typed);
  cache_mgr->session_id_ = msg_ack->session_id();
  cache_mgr->capabilities_ = msg_ack->capabilities();
  cache_mgr->max_object_size_ = msg_ack->max_object_size();
  assert(cache_mgr->max_object_size_ > 0);
  if (cache_mgr->max_object_size_ > kMaxSupportedObjectSize) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "external cache manager object size too large (%u)",
             cache_mgr->max_object_size_);
    return NULL;
  }
  if (cache_mgr->max_object_size_ < kMinSupportedObjectSize) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "external cache manager object size too small (%u)",
             cache_mgr->max_object_size_);
    return NULL;
  }
  if (msg_ack->has_pid())
    cache_mgr->pid_plugin_ = msg_ack->pid();
  return cache_mgr.Release();
}


/**
 * Tries to connect to the plugin at locator, or, if it doesn't exist, spawns
 * a new plugin using cmdline.  Two processes could try to spawn the plugin at
 * the same time.  In this case, the plugin should indicate to the client to
 * retry connecting.
 */
ExternalCacheManager::PluginHandle *ExternalCacheManager::CreatePlugin(
  const std::string &locator,
  const std::vector<std::string> &cmd_line)
{
  UniquePtr<PluginHandle> plugin_handle(new PluginHandle());
  unsigned num_attempts = 0;
  bool try_again = false;
  do {
    num_attempts++;
    if (num_attempts > 2) {
      // Prevent violate busy loops
      SafeSleepMs(1000);
    }
    plugin_handle->fd_connection_ = ConnectLocator(locator, num_attempts > 1);
    if (plugin_handle->IsValid()) {
      break;
    } else if (plugin_handle->fd_connection_ == -EINVAL) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "Invalid locator: %s", locator.c_str());
      plugin_handle->error_msg_ = "Invalid locator: " + locator;
      break;
    } else {
      if (num_attempts > 1) {
        LogCvmfs(kLogCache, kLogDebug | kLogStderr,
                 "Failed to connect to external cache manager: %d",
                 plugin_handle->fd_connection_);
      }
      plugin_handle->error_msg_ = "Failed to connect to external cache manager";
    }

    try_again = SpawnPlugin(cmd_line);
  } while (try_again);

  return plugin_handle.Release();
}


void ExternalCacheManager::CtrlTxn(
  const Label &label,
  const int flags,
  void *txn)
{
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->label = label;
  transaction->label_modified = true;
}


string ExternalCacheManager::Describe() {
  return "External cache manager\n";
}


bool ExternalCacheManager::DoFreeState(void *data) {
  FdTable<ReadOnlyHandle> *fd_table =
    reinterpret_cast<FdTable<ReadOnlyHandle> *>(data);
  delete fd_table;
  return true;
}


int ExternalCacheManager::DoOpen(const shash::Any &id) {
  int fd = -1;
  {
    WriteLockGuard guard(rwlock_fd_table_);
    fd = fd_table_.OpenFd(ReadOnlyHandle(id));
    if (fd < 0) {
      LogCvmfs(kLogCache, kLogDebug, "error while creating new fd: %s",
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


int ExternalCacheManager::DoRestoreState(void *data) {
  // When DoRestoreState is called, we have fd 0 assigned to the root file
  // catalog unless this is a lower layer cache in a tiered setup
  for (unsigned i = 1; i < fd_table_.GetMaxFds(); ++i) {
    assert(fd_table_.GetHandle(i) == ReadOnlyHandle());
  }
  ReadOnlyHandle handle_root = fd_table_.GetHandle(0);

  FdTable<ReadOnlyHandle> *other =
    reinterpret_cast<FdTable<ReadOnlyHandle> *>(data);
  fd_table_.AssignFrom(*other);
  cvmfs::MsgIoctl msg_ioctl;
  msg_ioctl.set_session_id(session_id_);
  msg_ioctl.set_conncnt_change_by(-1);
  CacheTransport::Frame frame(&msg_ioctl);
  transport_.SendFrame(&frame);

  int new_root_fd = -1;
  if (handle_root != ReadOnlyHandle()) {
    new_root_fd = fd_table_.OpenFd(handle_root);
    // There must be a free file descriptor because the root file catalog gets
    // closed before a reload
    assert(new_root_fd >= 0);
  }
  return new_root_fd;
}


void *ExternalCacheManager::DoSaveState() {
  cvmfs::MsgIoctl msg_ioctl;
  msg_ioctl.set_session_id(session_id_);
  msg_ioctl.set_conncnt_change_by(1);
  CacheTransport::Frame frame(&msg_ioctl);
  transport_.SendFrame(&frame);
  return fd_table_.Clone();
}


int ExternalCacheManager::Dup(int fd) {
  shash::Any id = GetHandle(fd);
  if (id == kInvalidHandle)
    return -EBADF;
  return DoOpen(id);
}


ExternalCacheManager::ExternalCacheManager(
  int fd_connection,
  unsigned max_open_fds)
  : pid_plugin_(0)
  , fd_table_(max_open_fds, ReadOnlyHandle())
  , transport_(fd_connection)
  , session_id_(-1)
  , max_object_size_(0)
  , spawned_(false)
  , terminated_(false)
  , capabilities_(cvmfs::CAP_NONE)
{
  int retval = pthread_rwlock_init(&rwlock_fd_table_, NULL);
  assert(retval == 0);
  retval = pthread_mutex_init(&lock_send_fd_, NULL);
  assert(retval == 0);
  retval = pthread_mutex_init(&lock_inflight_rpcs_, NULL);
  assert(retval == 0);
  memset(&thread_read_, 0, sizeof(thread_read_));
  atomic_init64(&next_request_id_);
}


ExternalCacheManager::~ExternalCacheManager() {
  terminated_ = true;
  MemoryFence();
  if (session_id_ >= 0) {
    cvmfs::MsgQuit msg_quit;
    msg_quit.set_session_id(session_id_);
    CacheTransport::Frame frame(&msg_quit);
    transport_.SendFrame(&frame);
  }
  shutdown(transport_.fd_connection(), SHUT_RDWR);
  if (spawned_)
    pthread_join(thread_read_, NULL);
  close(transport_.fd_connection());
  pthread_rwlock_destroy(&rwlock_fd_table_);
  pthread_mutex_destroy(&lock_send_fd_);
  pthread_mutex_destroy(&lock_inflight_rpcs_);
}


int ExternalCacheManager::Flush(bool do_commit, Transaction *transaction) {
  if (transaction->committed)
    return 0;
  LogCvmfs(kLogCache, kLogDebug, "flushing %u bytes for %s",
           transaction->buf_pos, transaction->id.ToString().c_str());
  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(transaction->id, &object_id);
  cvmfs::MsgStoreReq msg_store;
  msg_store.set_session_id(session_id_);
  msg_store.set_req_id(transaction->transaction_id);
  msg_store.set_allocated_object_id(&object_id);
  msg_store.set_part_nr((transaction->size / max_object_size_) + 1);
  msg_store.set_expected_size(transaction->expected_size);
  msg_store.set_last_part(do_commit);

  if (transaction->label_modified) {
    cvmfs::EnumObjectType object_type;
    transport_.FillObjectType(transaction->label.flags, &object_type);
    msg_store.set_object_type(object_type);
    msg_store.set_description(transaction->label.GetDescription());
  }

  RpcJob rpc_job(&msg_store);
  rpc_job.set_attachment_send(transaction->buffer, transaction->buf_pos);
  // TODO(jblomer): allow for out of order chunk upload
  CallRemotely(&rpc_job);
  msg_store.release_object_id();

  cvmfs::MsgStoreReply *msg_reply = rpc_job.msg_store_reply();
  if (msg_reply->status() == cvmfs::STATUS_OK) {
    transaction->flushed = true;
    if (do_commit)
      transaction->committed = true;
  }
  return Ack2Errno(msg_reply->status());
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


void *ExternalCacheManager::MainRead(void *data) {
  ExternalCacheManager *cache_mgr =
    reinterpret_cast<ExternalCacheManager *>(data);
  LogCvmfs(kLogCache, kLogDebug, "starting external cache reader thread");

  unsigned char buffer[cache_mgr->max_object_size_];
  while (true) {
    CacheTransport::Frame frame_recv;
    frame_recv.set_attachment(buffer, cache_mgr->max_object_size_);
    bool retval = cache_mgr->transport_.RecvFrame(&frame_recv);
    if (!retval)
      break;

    uint64_t req_id;
    uint64_t part_nr = 0;
    google::protobuf::MessageLite *msg = frame_recv.GetMsgTyped();
    if (msg->GetTypeName() == "cvmfs.MsgRefcountReply") {
      req_id = reinterpret_cast<cvmfs::MsgRefcountReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgObjectInfoReply") {
      req_id = reinterpret_cast<cvmfs::MsgObjectInfoReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgReadReply") {
      req_id = reinterpret_cast<cvmfs::MsgReadReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgStoreReply") {
      req_id = reinterpret_cast<cvmfs::MsgStoreReply *>(msg)->req_id();
      part_nr = reinterpret_cast<cvmfs::MsgStoreReply *>(msg)->part_nr();
    } else if (msg->GetTypeName() == "cvmfs.MsgInfoReply") {
      req_id = reinterpret_cast<cvmfs::MsgInfoReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgShrinkReply") {
      req_id = reinterpret_cast<cvmfs::MsgShrinkReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgListReply") {
      req_id = reinterpret_cast<cvmfs::MsgListReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgBreadcrumbReply") {
      req_id = reinterpret_cast<cvmfs::MsgBreadcrumbReply *>(msg)->req_id();
    } else if (msg->GetTypeName() == "cvmfs.MsgDetach") {
      // Release pinned catalogs
      cache_mgr->quota_mgr_->BroadcastBackchannels("R");
      continue;
    } else {
      PANIC(kLogSyslogErr | kLogDebug, "unexpected message %s",
            msg->GetTypeName().c_str());
    }

    RpcInFlight rpc_inflight;
    {
      MutexLockGuard guard(cache_mgr->lock_inflight_rpcs_);
      for (unsigned i = 0; i < cache_mgr->inflight_rpcs_.size(); ++i) {
        RpcJob *rpc_job = cache_mgr->inflight_rpcs_[i].rpc_job;
        if ((rpc_job->req_id() == req_id) && (rpc_job->part_nr() == part_nr)) {
          rpc_inflight = cache_mgr->inflight_rpcs_[i];
          cache_mgr->inflight_rpcs_.erase(
            cache_mgr->inflight_rpcs_.begin() + i);
          break;
        }
      }
    }
    if (rpc_inflight.rpc_job == NULL) {
      LogCvmfs(kLogCache, kLogSyslogWarn | kLogDebug,
               "got unmatched rpc reply");
      continue;
    }
    rpc_inflight.rpc_job->frame_recv()->MergeFrom(frame_recv);
    rpc_inflight.signal->Wakeup();
  }

  if (!cache_mgr->terminated_) {
    PANIC(kLogSyslogErr | kLogDebug,
          "connection to external cache manager broken (%d)", errno);
  }
  LogCvmfs(kLogCache, kLogDebug, "stopping external cache reader thread");
  return NULL;
}


int ExternalCacheManager::Open(const LabeledObject &object) {
  return DoOpen(object.id);
}


int ExternalCacheManager::OpenFromTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  LogCvmfs(kLogCache, kLogDebug, "open fd for transaction %s",
           transaction->id.ToString().c_str());
  int retval = Flush(true, transaction);
  if (retval != 0)
    return retval;

  int fd = -1;
  {
    WriteLockGuard guard(rwlock_fd_table_);
    fd = fd_table_.OpenFd(ReadOnlyHandle(transaction->id));
    if (fd < 0) {
      LogCvmfs(kLogCache, kLogDebug, "error while creating new fd: %s",
               strerror(-fd));
      return fd;
    }
  }
  transaction->open_fds++;
  return fd;
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
  uint64_t nbytes = 0;
  while (nbytes < size) {
    uint64_t batch_size =
      std::min(size - nbytes, static_cast<uint64_t>(max_object_size_));
    cvmfs::MsgReadReq msg_read;
    msg_read.set_session_id(session_id_);
    msg_read.set_req_id(NextRequestId());
    msg_read.set_allocated_object_id(&object_id);
    msg_read.set_offset(offset + nbytes);
    msg_read.set_size(batch_size);
    RpcJob rpc_job(&msg_read);
    rpc_job.set_attachment_recv(reinterpret_cast<char *>(buf) + nbytes,
                                batch_size);
    CallRemotely(&rpc_job);
    msg_read.release_object_id();

    cvmfs::MsgReadReply *msg_reply = rpc_job.msg_read_reply();
    if (msg_reply->status() == cvmfs::STATUS_OK) {
      nbytes += rpc_job.frame_recv()->att_size();
      // Fuse sends in rounded up buffers, so short reads are expected
      if (rpc_job.frame_recv()->att_size() < batch_size)
        return nbytes;
    } else {
      return Ack2Errno(msg_reply->status());
    }
  }
  return size;
}


int ExternalCacheManager::Readahead(int fd) {
  shash::Any id = GetHandle(fd);
  if (id == kInvalidHandle)
    return -EBADF;
  // No-op
  return 0;
}


int ExternalCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->buf_pos = 0;
  transaction->size = 0;
  transaction->open_fds = 0;
  transaction->committed = false;
  transaction->label_modified = true;

  if (!transaction->flushed)
    return 0;

  cvmfs::MsgHash object_id;
  transport_.FillMsgHash(transaction->id, &object_id);
  cvmfs::MsgStoreAbortReq msg_abort;
  msg_abort.set_session_id(session_id_);
  msg_abort.set_req_id(transaction->transaction_id);
  msg_abort.set_allocated_object_id(&object_id);
  RpcJob rpc_job(&msg_abort);
  CallRemotely(&rpc_job);
  msg_abort.release_object_id();
  cvmfs::MsgStoreReply *msg_reply = rpc_job.msg_store_reply();
  transaction->transaction_id = NextRequestId();
  transaction->flushed = false;
  return Ack2Errno(msg_reply->status());
}


manifest::Breadcrumb ExternalCacheManager::LoadBreadcrumb(
  const std::string &fqrn)
{
  if (!(capabilities_ & cvmfs::CAP_BREADCRUMB))
    return manifest::Breadcrumb();

  cvmfs::MsgBreadcrumbLoadReq msg_breadcrumb_load;
  msg_breadcrumb_load.set_session_id(session_id_);
  msg_breadcrumb_load.set_req_id(NextRequestId());
  msg_breadcrumb_load.set_fqrn(fqrn);
  RpcJob rpc_job(&msg_breadcrumb_load);
  CallRemotely(&rpc_job);

  manifest::Breadcrumb breadcrumb;
  cvmfs::MsgBreadcrumbReply *msg_reply = rpc_job.msg_breadcrumb_reply();
  if (msg_reply->status() == cvmfs::STATUS_OK) {
    assert(msg_reply->has_breadcrumb());
    assert(msg_reply->breadcrumb().fqrn() == fqrn);
    bool rv = transport_.ParseMsgHash(msg_reply->breadcrumb().hash(),
                                      &breadcrumb.catalog_hash);
    assert(rv);
    breadcrumb.catalog_hash.suffix = shash::kSuffixCatalog;
    breadcrumb.timestamp = msg_reply->breadcrumb().timestamp();
    if (msg_reply->breadcrumb().has_revision()) {
      breadcrumb.revision = msg_reply->breadcrumb().revision();
    } else {
      breadcrumb.revision = 0;
    }
  }
  return breadcrumb;
}


bool ExternalCacheManager::StoreBreadcrumb(const manifest::Manifest &manifest) {
  if (!(capabilities_ & cvmfs::CAP_BREADCRUMB))
    return false;

  cvmfs::MsgHash hash;
  transport_.FillMsgHash(manifest.catalog_hash(), &hash);
  cvmfs::MsgBreadcrumb breadcrumb;
  breadcrumb.set_fqrn(manifest.repository_name());
  breadcrumb.set_allocated_hash(&hash);
  breadcrumb.set_timestamp(manifest.publish_timestamp());
  breadcrumb.set_revision(manifest.revision());
  cvmfs::MsgBreadcrumbStoreReq msg_breadcrumb_store;
  msg_breadcrumb_store.set_session_id(session_id_);
  msg_breadcrumb_store.set_req_id(NextRequestId());
  msg_breadcrumb_store.set_allocated_breadcrumb(&breadcrumb);
  RpcJob rpc_job(&msg_breadcrumb_store);
  CallRemotely(&rpc_job);
  msg_breadcrumb_store.release_breadcrumb();
  breadcrumb.release_hash();

  cvmfs::MsgBreadcrumbReply *msg_reply = rpc_job.msg_breadcrumb_reply();
  return msg_reply->status() == cvmfs::STATUS_OK;
}


void ExternalCacheManager::Spawn() {
  int retval = pthread_create(&thread_read_, NULL, MainRead, this);
  assert(retval == 0);
  spawned_ = true;
}


/**
 * Returns true if the plugin could be spawned or was spawned by another
 * process.
 */
bool ExternalCacheManager::SpawnPlugin(const vector<string> &cmd_line) {
  if (cmd_line.empty())
    return false;

  int pipe_ready[2];
  MakePipe(pipe_ready);
  set<int> preserve_filedes;
  preserve_filedes.insert(pipe_ready[1]);

  int fd_null_read = open("/dev/null", O_RDONLY);
  int fd_null_write = open("/dev/null", O_WRONLY);
  assert((fd_null_read >= 0) && (fd_null_write >= 0));
  map<int, int> map_fildes;
  map_fildes[fd_null_read] = 0;
  map_fildes[fd_null_write] = 1;
  map_fildes[fd_null_write] = 2;

  pid_t child_pid;
  int retval = setenv(CacheTransport::kEnvReadyNotifyFd,
                      StringifyInt(pipe_ready[1]).c_str(), 1);
  assert(retval == 0);
  retval = ManagedExec(cmd_line,
                       preserve_filedes,
                       map_fildes,
                       false,  // drop_credentials
                       false,  // clear_env
                       true,   // double fork
                       &child_pid);
  unsetenv(CacheTransport::kEnvReadyNotifyFd);
  close(fd_null_read);
  close(fd_null_write);
  if (!retval) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "failed to start cache plugin '%s'",
             JoinStrings(cmd_line, " ").c_str());
    ClosePipe(pipe_ready);
    return false;
  }

  LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
           "started cache plugin '%s' (pid %d), waiting for it to become ready",
           JoinStrings(cmd_line, " ").c_str(), child_pid);
  close(pipe_ready[1]);
  char buf;
  if (read(pipe_ready[0], &buf, 1) != 1) {
    close(pipe_ready[0]);
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "cache plugin did not start properly");
    return false;
  }
  close(pipe_ready[0]);

  if (buf == CacheTransport::kReadyNotification)
    return true;
  LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
           "cache plugin failed to create an endpoint");
  return false;
}


int ExternalCacheManager::StartTxn(
  const shash::Any &id,
  uint64_t size,
  void *txn)
{
  if (!(capabilities_ & cvmfs::CAP_WRITE))
    return -EROFS;

  Transaction *transaction = new (txn) Transaction(id);
  transaction->expected_size = size;
  transaction->transaction_id = NextRequestId();
#ifdef __APPLE__
  transaction->buffer =
    reinterpret_cast<unsigned char *>(smalloc(max_object_size_));
#endif
  return 0;
}


int64_t ExternalCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  assert(!transaction->committed);
  LogCvmfs(kLogCache, kLogDebug, "writing %" PRIu64 " bytes for %s",
           size, transaction->id.ToString().c_str());

  if (transaction->expected_size != kSizeUnknown) {
    if (transaction->size + size > transaction->expected_size) {
      LogCvmfs(kLogCache, kLogDebug,
               "Transaction size (%" PRIu64 ") > expected size (%" PRIu64 ")",
               transaction->size + size, transaction->expected_size);
      return -EFBIG;
    }
  }

  uint64_t written = 0;
  const unsigned char *read_pos = reinterpret_cast<const unsigned char *>(buf);
  while (written < size) {
    if (transaction->buf_pos == max_object_size_) {
      bool do_commit = false;
      if (transaction->expected_size != kSizeUnknown)
        do_commit = (transaction->size + written) == transaction->expected_size;
      int retval = Flush(do_commit, transaction);
      if (retval != 0) {
        transaction->size += written;
        return retval;
      }
      transaction->size += transaction->buf_pos;
      transaction->buf_pos = 0;
    }
    uint64_t remaining = size - written;
    uint64_t space_in_buffer = max_object_size_ - transaction->buf_pos;
    uint64_t batch_size = std::min(remaining, space_in_buffer);
    memcpy(transaction->buffer + transaction->buf_pos, read_pos, batch_size);
    transaction->buf_pos += batch_size;
    written += batch_size;
    read_pos += batch_size;
  }
  return written;
}


//------------------------------------------------------------------------------


bool ExternalQuotaManager::DoListing(
  cvmfs::EnumObjectType type,
  vector<cvmfs::MsgListRecord> *result)
{
  if (!(cache_mgr_->capabilities_ & cvmfs::CAP_LIST))
    return false;

  uint64_t listing_id = 0;
  bool more_data = false;
  do {
    cvmfs::MsgListReq msg_list;
    msg_list.set_session_id(cache_mgr_->session_id_);
    msg_list.set_req_id(cache_mgr_->NextRequestId());
    msg_list.set_listing_id(listing_id);
    msg_list.set_object_type(type);
    ExternalCacheManager::RpcJob rpc_job(&msg_list);
    cache_mgr_->CallRemotely(&rpc_job);

    cvmfs::MsgListReply *msg_reply = rpc_job.msg_list_reply();
    if (msg_reply->status() != cvmfs::STATUS_OK)
      return false;
    more_data = !msg_reply->is_last_part();
    listing_id = msg_reply->listing_id();
    for (int i = 0; i < msg_reply->list_record_size(); ++i) {
      result->push_back(msg_reply->list_record(i));
    }
  } while (more_data);

  return true;
}


bool ExternalQuotaManager::Cleanup(const uint64_t leave_size) {
  if (!(cache_mgr_->capabilities_ & cvmfs::CAP_SHRINK))
    return false;

  cvmfs::MsgShrinkReq msg_shrink;
  msg_shrink.set_session_id(cache_mgr_->session_id_);
  msg_shrink.set_req_id(cache_mgr_->NextRequestId());
  msg_shrink.set_shrink_to(leave_size);
  ExternalCacheManager::RpcJob rpc_job(&msg_shrink);
  cache_mgr_->CallRemotely(&rpc_job);

  cvmfs::MsgShrinkReply *msg_reply = rpc_job.msg_shrink_reply();
  return msg_reply->status() == cvmfs::STATUS_OK;
}


ExternalQuotaManager *ExternalQuotaManager::Create(
  ExternalCacheManager *cache_mgr)
{
  UniquePtr<ExternalQuotaManager> quota_mgr(
    new ExternalQuotaManager(cache_mgr));
  assert(quota_mgr.IsValid());

  return quota_mgr.Release();
}


int ExternalQuotaManager::GetInfo(QuotaInfo *quota_info) {
  if (!(cache_mgr_->capabilities_ & cvmfs::CAP_INFO))
    return Ack2Errno(cvmfs::STATUS_NOSUPPORT);

  cvmfs::MsgInfoReq msg_info;
  msg_info.set_session_id(cache_mgr_->session_id_);
  msg_info.set_req_id(cache_mgr_->NextRequestId());
  ExternalCacheManager::RpcJob rpc_job(&msg_info);
  cache_mgr_->CallRemotely(&rpc_job);

  cvmfs::MsgInfoReply *msg_reply = rpc_job.msg_info_reply();
  if (msg_reply->status() == cvmfs::STATUS_OK) {
    quota_info->size = msg_reply->size_bytes();
    quota_info->used = msg_reply->used_bytes();
    quota_info->pinned = msg_reply->pinned_bytes();
    if (msg_reply->no_shrink() >= 0)
      quota_info->no_shrink = msg_reply->no_shrink();
  }
  return Ack2Errno(msg_reply->status());
}


uint64_t ExternalQuotaManager::GetCapacity() {
  QuotaInfo info;
  int retval = GetInfo(&info);
  if (retval != 0)
    return uint64_t(-1);
  return info.size;
}


uint64_t ExternalQuotaManager::GetCleanupRate(uint64_t period_s) {
  QuotaInfo info;
  int retval = GetInfo(&info);
  if (retval != 0)
    return 0;
  return info.no_shrink;
}


uint64_t ExternalQuotaManager::GetSize() {
  QuotaInfo info;
  int retval = GetInfo(&info);
  if (retval != 0)
    return 0;
  return info.used;
}


uint64_t ExternalQuotaManager::GetSizePinned() {
  QuotaInfo info;
  int retval = GetInfo(&info);
  if (retval != 0)
    return 0;
  return info.pinned;
}


bool ExternalQuotaManager::HasCapability(Capabilities capability) {
  switch (capability) {
    case kCapIntrospectSize:
      return cache_mgr_->capabilities_ & cvmfs::CAP_INFO;
    case kCapIntrospectCleanupRate:
      return cache_mgr_->capabilities_ & cvmfs::CAP_SHRINK_RATE;
    case kCapList:
      return cache_mgr_->capabilities_ & cvmfs::CAP_LIST;
    case kCapShrink:
      return cache_mgr_->capabilities_ & cvmfs::CAP_SHRINK;
    case kCapListeners:
      return true;
    default:
      return false;
  }
}


vector<string> ExternalQuotaManager::List() {
  vector<string> result;
  vector<cvmfs::MsgListRecord> raw_list;
  bool retval = DoListing(cvmfs::OBJECT_REGULAR, &raw_list);
  if (!retval)
    return result;
  for (unsigned i = 0; i < raw_list.size(); ++i)
    result.push_back(raw_list[i].description());
  return result;
}


vector<string> ExternalQuotaManager::ListCatalogs() {
  vector<string> result;
  vector<cvmfs::MsgListRecord> raw_list;
  bool retval = DoListing(cvmfs::OBJECT_CATALOG, &raw_list);
  if (!retval)
    return result;
  for (unsigned i = 0; i < raw_list.size(); ++i)
    result.push_back(raw_list[i].description());
  return result;
}


vector<string> ExternalQuotaManager::ListPinned() {
  vector<string> result;
  vector<cvmfs::MsgListRecord> raw_lists[3];
  bool retval = DoListing(cvmfs::OBJECT_REGULAR, &raw_lists[0]);
  if (!retval)
    return result;
  retval = DoListing(cvmfs::OBJECT_CATALOG, &raw_lists[1]);
  if (!retval)
    return result;
  retval = DoListing(cvmfs::OBJECT_VOLATILE, &raw_lists[2]);
  if (!retval)
    return result;
  for (unsigned i = 0; i < sizeof(raw_lists) / sizeof(raw_lists[0]); ++i) {
    for (unsigned j = 0; j < raw_lists[i].size(); ++j) {
      if (raw_lists[i][j].pinned())
        result.push_back(raw_lists[i][j].description());
    }
  }
  return result;
}


vector<string> ExternalQuotaManager::ListVolatile() {
  vector<string> result;
  vector<cvmfs::MsgListRecord> raw_list;
  bool retval = DoListing(cvmfs::OBJECT_VOLATILE, &raw_list);
  if (!retval)
    return result;
  for (unsigned i = 0; i < raw_list.size(); ++i)
    result.push_back(raw_list[i].description());
  return result;
}


void ExternalQuotaManager::RegisterBackChannel(
  int back_channel[2],
  const string &channel_id)
{
  shash::Md5 hash_id = shash::Md5(shash::AsciiPtr(channel_id));
  MakePipe(back_channel);
  LockBackChannels();
  assert(back_channels_.find(hash_id) == back_channels_.end());
  back_channels_[hash_id] = back_channel[1];
  UnlockBackChannels();
}


void ExternalQuotaManager::UnregisterBackChannel(
  int back_channel[2],
  const string &channel_id)
{
  shash::Md5 hash_id = shash::Md5(shash::AsciiPtr(channel_id));
  LockBackChannels();
  back_channels_.erase(hash_id);
  UnlockBackChannels();
  ClosePipe(back_channel);
}
