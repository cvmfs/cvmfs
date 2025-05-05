/**
 * This file is part of the CernVM File System.
 */

#include "channel.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <vector>

#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT


SessionCtx *SessionCtx::instance_ = NULL;

void SessionCtx::CleanupInstance() {
  delete instance_;
  instance_ = NULL;
}


SessionCtx::SessionCtx() {
  lock_tls_blocks_ = reinterpret_cast<pthread_mutex_t *>(
    smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_tls_blocks_, NULL);
  assert(retval == 0);
}


SessionCtx::~SessionCtx() {
  pthread_mutex_destroy(lock_tls_blocks_);
  free(lock_tls_blocks_);

  for (unsigned i = 0; i < tls_blocks_.size(); ++i) {
    delete tls_blocks_[i];
  }

  int retval = pthread_key_delete(thread_local_storage_);
  assert(retval == 0);
}


SessionCtx *SessionCtx::GetInstance() {
  if (instance_ == NULL) {
    instance_ = new SessionCtx();
    int retval =
      pthread_key_create(&instance_->thread_local_storage_, TlsDestructor);
    assert(retval == 0);
  }

  return instance_;
}


void SessionCtx::Get(uint64_t *id, char **reponame, char **client_instance) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));
  if ((tls == NULL) || !tls->is_set) {
    *id = 0;
    *reponame = NULL;
    *client_instance = NULL;
  } else {
    *id = tls->id;
    *reponame = tls->reponame;
    *client_instance = tls->client_instance;
  }
}


bool SessionCtx::IsSet() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));
  if (tls == NULL)
    return false;

  return tls->is_set;
}


void SessionCtx::Set(uint64_t id, char *reponame, char *client_instance) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));

  if (tls == NULL) {
    tls = new ThreadLocalStorage(id, reponame, client_instance);
    int retval = pthread_setspecific(thread_local_storage_, tls);
    assert(retval == 0);
    MutexLockGuard lock_guard(lock_tls_blocks_);
    tls_blocks_.push_back(tls);
  } else {
    tls->id = id;
    tls->reponame = reponame;
    tls->client_instance = client_instance;
    tls->is_set = true;
  }
}


void SessionCtx::TlsDestructor(void *data) {
  ThreadLocalStorage *tls = static_cast<SessionCtx::ThreadLocalStorage *>(data);
  delete tls;

  assert(instance_);
  MutexLockGuard lock_guard(instance_->lock_tls_blocks_);
  for (vector<ThreadLocalStorage *>::iterator i =
       instance_->tls_blocks_.begin(), iEnd = instance_->tls_blocks_.end();
       i != iEnd; ++i)
  {
    if ((*i) == tls) {
      instance_->tls_blocks_.erase(i);
      break;
    }
  }
}


void SessionCtx::Unset() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));
  if (tls != NULL) {
    tls->is_set = false;
    tls->id = 0;
    tls->reponame = NULL;
    tls->client_instance = NULL;
  }
}


//------------------------------------------------------------------------------


CachePlugin::SessionInfo::SessionInfo(uint64_t id, const std::string &name)
  : id(id)
  , name(name)
{
  vector<string> tokens = SplitString(name, ':');
  reponame = strdup(tokens[0].c_str());
  if (tokens.size() > 1)
    client_instance = strdup(tokens[1].c_str());
  else
    client_instance = NULL;
}

const uint64_t CachePlugin::kSizeUnknown = uint64_t(-1);


void CachePlugin::AskToDetach() {
  char detach = kSignalDetach;
  WritePipe(pipe_ctrl_[1], &detach, 1);
}


CachePlugin::CachePlugin(uint64_t capabilities)
  : is_local_(false)
  , capabilities_(capabilities)
  , fd_socket_(-1)
  , fd_socket_lock_(-1)
  , running_(0)
  , num_workers_(0)
  , max_object_size_(kDefaultMaxObjectSize)
  , num_inlimbo_clients_(0)
{
  atomic_init64(&next_session_id_);
  atomic_init64(&next_txn_id_);
  atomic_init64(&next_lst_id_);
  // Don't use listing id zero
  atomic_inc64(&next_lst_id_);
  txn_ids_.Init(128, UniqueRequest(), HashUniqueRequest);
  memset(&thread_io_, 0, sizeof(thread_io_));
  MakePipe(pipe_ctrl_);
}


CachePlugin::~CachePlugin() {
  Terminate();
  ClosePipe(pipe_ctrl_);
  if (fd_socket_ >= 0)
    close(fd_socket_);
  if (fd_socket_lock_ >= 0)
    UnlockFile(fd_socket_lock_);
}


void CachePlugin::HandleBreadcrumbStore(
  cvmfs::MsgBreadcrumbStoreReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgBreadcrumbReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  manifest::Breadcrumb breadcrumb;
  bool retval = transport->ParseMsgHash(msg_req->breadcrumb().hash(),
                                        &breadcrumb.catalog_hash);
  if (!retval) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed hash received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    breadcrumb.timestamp = msg_req->breadcrumb().timestamp();
    if (msg_req->breadcrumb().has_revision()) {
      breadcrumb.revision = msg_req->breadcrumb().revision();
    } else {
      breadcrumb.revision = 0;
    }
    cvmfs::EnumStatus status =
      StoreBreadcrumb(msg_req->breadcrumb().fqrn(), breadcrumb);
    msg_reply.set_status(status);
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleBreadcrumbLoad(
  cvmfs::MsgBreadcrumbLoadReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgBreadcrumbReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  manifest::Breadcrumb breadcrumb;
  cvmfs::EnumStatus status =
    LoadBreadcrumb(msg_req->fqrn(), &breadcrumb);
  msg_reply.set_status(status);
  if (status == cvmfs::STATUS_OK) {
    assert(breadcrumb.IsValid());
    cvmfs::MsgHash *msg_hash = new cvmfs::MsgHash();
    transport->FillMsgHash(breadcrumb.catalog_hash, msg_hash);
    cvmfs::MsgBreadcrumb *msg_breadcrumb = new cvmfs::MsgBreadcrumb();
    msg_breadcrumb->set_fqrn(msg_req->fqrn());
    msg_breadcrumb->set_allocated_hash(msg_hash);
    msg_breadcrumb->set_timestamp(breadcrumb.timestamp);
    msg_breadcrumb->set_revision(breadcrumb.revision);
    msg_reply.set_allocated_breadcrumb(msg_breadcrumb);
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleHandshake(
  cvmfs::MsgHandshake *msg_req,
  CacheTransport *transport)
{
  uint64_t session_id = NextSessionId();
  if (msg_req->has_name()) {
    sessions_[session_id] = SessionInfo(session_id, msg_req->name());
  } else {
    sessions_[session_id] = SessionInfo(session_id,
      "anonymous client (" + StringifyInt(session_id) + ")");
  }
  cvmfs::MsgHandshakeAck msg_ack;
  CacheTransport::Frame frame_send(&msg_ack);

  msg_ack.set_status(cvmfs::STATUS_OK);
  msg_ack.set_name(name_);
  msg_ack.set_protocol_version(kPbProtocolVersion);
  msg_ack.set_max_object_size(max_object_size_);
  msg_ack.set_session_id(session_id);
  msg_ack.set_capabilities(capabilities_);
  if (is_local_)
    msg_ack.set_pid(getpid());
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleInfo(
  cvmfs::MsgInfoReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgInfoReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  Info info;
  cvmfs::EnumStatus status = GetInfo(&info);
  if (status != cvmfs::STATUS_OK) {
    LogSessionError(msg_req->session_id(), status,
                    "failed to query cache status");
  }
  msg_reply.set_size_bytes(info.size_bytes);
  msg_reply.set_used_bytes(info.used_bytes);
  msg_reply.set_pinned_bytes(info.pinned_bytes);
  msg_reply.set_no_shrink(info.no_shrink);
  msg_reply.set_status(status);
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleIoctl(cvmfs::MsgIoctl *msg_req) {
  if (!msg_req->has_conncnt_change_by())
    return;
  int32_t conncnt_change_by = msg_req->conncnt_change_by();
  if ((static_cast<int32_t>(num_inlimbo_clients_) + conncnt_change_by) < 0) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "invalid request to drop connection counter below zero");
    return;
  }
  if (conncnt_change_by > 0) {
    LogSessionInfo(msg_req->session_id(), "lock session beyond lifetime");
  } else {
    LogSessionInfo(msg_req->session_id(), "release session lock");
  }
  num_inlimbo_clients_ += conncnt_change_by;
}


void CachePlugin::HandleList(
  cvmfs::MsgListReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgListReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  int64_t listing_id = msg_req->listing_id();
  msg_reply.set_listing_id(listing_id);
  msg_reply.set_is_last_part(true);

  cvmfs::EnumStatus status;
  if (msg_req->listing_id() == 0) {
    listing_id = NextLstId();
    status = ListingBegin(listing_id, msg_req->object_type());
    if (status != cvmfs::STATUS_OK) {
      LogSessionError(msg_req->session_id(), status,
                      "failed to start enumeration of objects");
      msg_reply.set_status(status);
      transport->SendFrame(&frame_send);
      return;
    }
    msg_reply.set_listing_id(listing_id);
  }
  assert(listing_id != 0);

  ObjectInfo item;
  unsigned total_size = 0;
  while ((status = ListingNext(listing_id, &item)) == cvmfs::STATUS_OK) {
    cvmfs::MsgListRecord *msg_list_record = msg_reply.add_list_record();
    cvmfs::MsgHash *msg_hash = new cvmfs::MsgHash();
    transport->FillMsgHash(item.id, msg_hash);
    msg_list_record->set_allocated_hash(msg_hash);
    msg_list_record->set_pinned(item.pinned);
    msg_list_record->set_description(item.description);
    // Approximation of the message size
    total_size += sizeof(item) + item.description.length();
    if (total_size > kListingSize)
      break;
  }
  if (status == cvmfs::STATUS_OUTOFBOUNDS) {
    ListingEnd(listing_id);
    status = cvmfs::STATUS_OK;
  } else {
    msg_reply.set_is_last_part(false);
  }
  if (status != cvmfs::STATUS_OK) {
    LogSessionError(msg_req->session_id(), status, "failed enumerate objects");
  }
  msg_reply.set_status(status);
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleObjectInfo(
  cvmfs::MsgObjectInfoReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgObjectInfoReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed hash received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    ObjectInfo info;
    cvmfs::EnumStatus status = GetObjectInfo(object_id, &info);
    msg_reply.set_status(status);
    if (status == cvmfs::STATUS_OK) {
      msg_reply.set_object_type(info.object_type);
      msg_reply.set_size(info.size);
    } else if (status != cvmfs::STATUS_NOENTRY) {
      LogSessionError(msg_req->session_id(), status,
                      "failed retrieving object details");
    }
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleRead(
  cvmfs::MsgReadReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgReadReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval || (msg_req->size() > max_object_size_)) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed hash received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    transport->SendFrame(&frame_send);
    return;
  }
  unsigned size = msg_req->size();
#ifdef __APPLE__
  unsigned char *buffer = reinterpret_cast<unsigned char *>(smalloc(size));
#else
  unsigned char buffer[size];
#endif
  cvmfs::EnumStatus status = Pread(object_id, msg_req->offset(), &size, buffer);
  msg_reply.set_status(status);
  if (status == cvmfs::STATUS_OK) {
    frame_send.set_attachment(buffer, size);
  } else {
    LogSessionError(msg_req->session_id(), status,
                    "failed to read from object");
  }
  transport->SendFrame(&frame_send);
#ifdef __APPLE__
  free(buffer);
#endif
}


void CachePlugin::HandleRefcount(
  cvmfs::MsgRefcountReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgRefcountReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed hash received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    cvmfs::EnumStatus status = ChangeRefcount(object_id, msg_req->change_by());
    msg_reply.set_status(status);
    if ((status != cvmfs::STATUS_OK) && (status != cvmfs::STATUS_NOENTRY)) {
      LogSessionError(msg_req->session_id(), status,
                      "failed to open/close object " + object_id.ToString());
    }
  }
  transport->SendFrame(&frame_send);
}


bool CachePlugin::HandleRequest(int fd_con) {
  CacheTransport transport(fd_con, CacheTransport::kFlagSendIgnoreFailure);
  char buffer[max_object_size_];
  CacheTransport::Frame frame_recv;
  frame_recv.set_attachment(buffer, max_object_size_);
  bool retval = transport.RecvFrame(&frame_recv);
  if (!retval) {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "failed to receive request from connection (%d)", errno);
    return false;
  }

  google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();

  if (msg_typed->GetTypeName() == "cvmfs.MsgHandshake") {
    cvmfs::MsgHandshake *msg_req =
      reinterpret_cast<cvmfs::MsgHandshake *>(msg_typed);
    HandleHandshake(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
    cvmfs::MsgQuit *msg_req = reinterpret_cast<cvmfs::MsgQuit *>(msg_typed);
    map<uint64_t, SessionInfo>::const_iterator iter =
      sessions_.find(msg_req->session_id());
    if (iter != sessions_.end()) {
      free(iter->second.reponame);
      free(iter->second.client_instance);
    }
    sessions_.erase(msg_req->session_id());
    return false;
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgIoctl") {
    HandleIoctl(reinterpret_cast<cvmfs::MsgIoctl *>(msg_typed));
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgRefcountReq") {
    cvmfs::MsgRefcountReq *msg_req =
      reinterpret_cast<cvmfs::MsgRefcountReq *>(msg_typed);
    HandleRefcount(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgObjectInfoReq") {
    cvmfs::MsgObjectInfoReq *msg_req =
      reinterpret_cast<cvmfs::MsgObjectInfoReq *>(msg_typed);
    HandleObjectInfo(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgReadReq") {
    cvmfs::MsgReadReq *msg_req =
      reinterpret_cast<cvmfs::MsgReadReq *>(msg_typed);
    HandleRead(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgStoreReq") {
    cvmfs::MsgStoreReq *msg_req =
      reinterpret_cast<cvmfs::MsgStoreReq *>(msg_typed);
    HandleStore(msg_req, &frame_recv, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgStoreAbortReq") {
    cvmfs::MsgStoreAbortReq *msg_req =
      reinterpret_cast<cvmfs::MsgStoreAbortReq *>(msg_typed);
    HandleStoreAbort(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgInfoReq") {
    cvmfs::MsgInfoReq *msg_req =
      reinterpret_cast<cvmfs::MsgInfoReq *>(msg_typed);
    HandleInfo(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgShrinkReq") {
    cvmfs::MsgShrinkReq *msg_req =
      reinterpret_cast<cvmfs::MsgShrinkReq *>(msg_typed);
    HandleShrink(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgListReq") {
    cvmfs::MsgListReq *msg_req =
      reinterpret_cast<cvmfs::MsgListReq *>(msg_typed);
    HandleList(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgBreadcrumbStoreReq") {
    cvmfs::MsgBreadcrumbStoreReq *msg_req =
      reinterpret_cast<cvmfs::MsgBreadcrumbStoreReq *>(msg_typed);
    HandleBreadcrumbStore(msg_req, &transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgBreadcrumbLoadReq") {
    cvmfs::MsgBreadcrumbLoadReq *msg_req =
      reinterpret_cast<cvmfs::MsgBreadcrumbLoadReq *>(msg_typed);
    HandleBreadcrumbLoad(msg_req, &transport);
  } else {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "unexpected message from client: %s",
             std::string(msg_typed->GetTypeName()).c_str());
    return false;
  }

  return true;
}


void CachePlugin::HandleShrink(
  cvmfs::MsgShrinkReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgShrinkReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  uint64_t used_bytes = 0;
  cvmfs::EnumStatus status = Shrink(msg_req->shrink_to(), &used_bytes);
  msg_reply.set_used_bytes(used_bytes);
  msg_reply.set_status(status);
  if ((status != cvmfs::STATUS_OK) && (status != cvmfs::STATUS_PARTIAL)) {
    LogSessionError(msg_req->session_id(), status, "failed to cleanup cache");
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleStoreAbort(
  cvmfs::MsgStoreAbortReq *msg_req,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgStoreReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);
  msg_reply.set_req_id(msg_req->req_id());
  msg_reply.set_part_nr(0);
  uint64_t txn_id;
  UniqueRequest uniq_req(msg_req->session_id(), msg_req->req_id());
  bool retval = txn_ids_.Lookup(uniq_req, &txn_id);
  if (!retval) {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed transaction id received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    cvmfs::EnumStatus status = AbortTxn(txn_id);
    msg_reply.set_status(status);
    if (status != cvmfs::STATUS_OK) {
      LogSessionError(msg_req->session_id(), status,
                      "failed to abort transaction");
    }
    txn_ids_.Erase(uniq_req);
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleStore(
  cvmfs::MsgStoreReq *msg_req,
  CacheTransport::Frame *frame,
  CacheTransport *transport)
{
  SessionCtxGuard session_guard(msg_req->session_id(), this);
  cvmfs::MsgStoreReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);
  msg_reply.set_req_id(msg_req->req_id());
  msg_reply.set_part_nr(msg_req->part_nr());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if ( !retval ||
       (frame->att_size() > max_object_size_) ||
       ((frame->att_size() < max_object_size_) && !msg_req->last_part()) )
  {
    LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                    "malformed hash or bad object size received from client");
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    transport->SendFrame(&frame_send);
    return;
  }

  UniqueRequest uniq_req(msg_req->session_id(), msg_req->req_id());
  uint64_t txn_id;
  cvmfs::EnumStatus status = cvmfs::STATUS_OK;
  if (msg_req->part_nr() == 1) {
    if (txn_ids_.Contains(uniq_req)) {
      LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                      "invalid attempt to restart running transaction");
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
      transport->SendFrame(&frame_send);
      return;
    }
    txn_id = NextTxnId();
    ObjectInfo info;
    info.id = object_id;
    if (msg_req->has_expected_size()) {info.size = msg_req->expected_size();}
    if (msg_req->has_object_type()) {info.object_type = msg_req->object_type();}
    if (msg_req->has_description()) {info.description = msg_req->description();}
    status = StartTxn(object_id, txn_id, info);
    if (status != cvmfs::STATUS_OK) {
      LogSessionError(msg_req->session_id(), status,
                      "failed to start transaction");
      msg_reply.set_status(status);
      transport->SendFrame(&frame_send);
      return;
    }
    txn_ids_.Insert(uniq_req, txn_id);
  } else {
    retval = txn_ids_.Lookup(uniq_req, &txn_id);
    if (!retval) {
      LogSessionError(msg_req->session_id(), cvmfs::STATUS_MALFORMED,
                      "invalid transaction received from client");
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
      transport->SendFrame(&frame_send);
      return;
    }
  }

  // TODO(jblomer): check part number and send objects up in order
  if (frame->att_size() > 0) {
    status = WriteTxn(txn_id,
                      reinterpret_cast<unsigned char *>(frame->attachment()),
                      frame->att_size());
    if (status != cvmfs::STATUS_OK) {
      LogSessionError(msg_req->session_id(), status, "failure writing object");
      msg_reply.set_status(status);
      transport->SendFrame(&frame_send);
      return;
    }
  }

  if (msg_req->last_part()) {
    status = CommitTxn(txn_id);
    if (status != cvmfs::STATUS_OK) {
      LogSessionError(msg_req->session_id(), status,
                      "failure committing object");
    }
    txn_ids_.Erase(uniq_req);
  }
  msg_reply.set_status(status);
  transport->SendFrame(&frame_send);
}


bool CachePlugin::IsRunning() {
  return atomic_read32(&running_) != 0;
}


bool CachePlugin::Listen(const string &locator) {
  vector<string> tokens = SplitString(locator, '=');
  if (tokens[0] == "unix") {
    string lock_path = tokens[1] + ".lock";
    fd_socket_lock_ = TryLockFile(lock_path);
    if (fd_socket_lock_ == -1) {
      LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
               "failed to acquire lock file %s (%d)", lock_path.c_str(), errno);
      NotifySupervisor(CacheTransport::kFailureNotification);
      return false;
    } else if (fd_socket_lock_ == -2) {
      // Another plugin process probably started in the meantime
      NotifySupervisor(CacheTransport::kReadyNotification);
      if (getenv(CacheTransport::kEnvReadyNotifyFd) == NULL) {
        LogCvmfs(kLogCache, kLogSyslogErr | kLogStderr,
                 "failed to lock on %s, file is busy", lock_path.c_str());
      }
      return false;
    }
    assert(fd_socket_lock_ >= 0);
    fd_socket_ = MakeSocket(tokens[1], 0600);
    is_local_ = true;
  } else if (tokens[0] == "tcp") {
    vector<string> tcp_address = SplitString(tokens[1], ':');
    if (tcp_address.size() != 2) {
      LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
               "invalid locator: %s", locator.c_str());
      NotifySupervisor(CacheTransport::kFailureNotification);
      return false;
    }
    fd_socket_ = MakeTcpEndpoint(tcp_address[0], String2Uint64(tcp_address[1]));
  } else {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "unknown endpoint in locator: %s", locator.c_str());
    NotifySupervisor(CacheTransport::kFailureNotification);
    return false;
  }

  if (fd_socket_ < 0) {
    if (errno == EADDRINUSE) {
      // Another plugin process probably started in the meantime
      NotifySupervisor(CacheTransport::kReadyNotification);
    } else {
      LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
               "failed to create endpoint %s (%d)", locator.c_str(), errno);
      NotifySupervisor(CacheTransport::kFailureNotification);
    }
    is_local_ = false;
    return false;
  }
  int retval = listen(fd_socket_, 32);
  assert(retval == 0);

  return true;
}


void CachePlugin::LogSessionInfo(uint64_t session_id, const string &msg) {
  string session_str("unidentified client (" + StringifyInt(session_id) + ")");
  map<uint64_t, SessionInfo>::const_iterator iter = sessions_.find(session_id);
  if (iter != sessions_.end()) {
    session_str = iter->second.name;
  }
  LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
           "session '%s': %s", session_str.c_str(), msg.c_str());
}


void CachePlugin::LogSessionError(
  uint64_t session_id,
  cvmfs::EnumStatus status,
  const std::string &msg)
{
  string session_str("unidentified client (" + StringifyInt(session_id) + ")");
  map<uint64_t, SessionInfo>::const_iterator iter = sessions_.find(session_id);
  if (iter != sessions_.end()) {
    session_str = iter->second.name;
  }
  LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
           "session '%s': %s (%d - %s)",
           session_str.c_str(), msg.c_str(), status,
           CacheTransportCode2Ascii(status));
}


void *CachePlugin::MainProcessRequests(void *data) {
  CachePlugin *cache_plugin = reinterpret_cast<CachePlugin *>(data);

  platform_sighandler_t save_sigpipe = signal(SIGPIPE, SIG_IGN);

  vector<struct pollfd> watch_fds;
  // Elements 0, 1: control pipe, socket fd
  struct pollfd watch_ctrl;
  watch_ctrl.fd = cache_plugin->pipe_ctrl_[0];
  watch_ctrl.events = POLLIN | POLLPRI;
  watch_fds.push_back(watch_ctrl);
  struct pollfd watch_socket;
  watch_socket.fd = cache_plugin->fd_socket_;
  watch_socket.events = POLLIN | POLLPRI;
  watch_fds.push_back(watch_socket);

  bool terminated = false;
  while (!terminated) {
    for (unsigned i = 0; i < watch_fds.size(); ++i)
      watch_fds[i].revents = 0;
    int retval = poll(&watch_fds[0], watch_fds.size(), -1);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      PANIC(kLogSyslogErr | kLogDebug, "cache plugin connection failure (%d)",
            errno);
    }

    // Termination or detach
    if (watch_fds[0].revents) {
      char signal;
      ReadPipe(watch_fds[0].fd, &signal, 1);
      if (signal == kSignalDetach) {
        cache_plugin->SendDetachRequests();
        continue;
      }

      // termination
      if (watch_fds.size() > 2) {
        LogCvmfs(kLogCache, kLogSyslogWarn | kLogDebug,
                 "terminating external cache manager with pending connections");
      }
      break;
    }

    // New connection
    if (watch_fds[1].revents) {
      struct sockaddr_un remote;
      socklen_t socket_size = sizeof(remote);
      int fd_con =
        accept(watch_fds[1].fd, (struct sockaddr *)&remote, &socket_size);
      if (fd_con < 0) {
        LogCvmfs(kLogCache, kLogSyslogWarn | kLogDebug,
                 "failed to establish connection (%d)", errno);
        continue;
      }
      struct pollfd watch_con;
      watch_con.fd = fd_con;
      watch_con.events = POLLIN | POLLPRI;
      watch_fds.push_back(watch_con);
      cache_plugin->connections_.insert(fd_con);
    }

    // New request
    for (unsigned i = 2; i < watch_fds.size(); ) {
      if (watch_fds[i].revents) {
        bool proceed = cache_plugin->HandleRequest(watch_fds[i].fd);
        if (!proceed) {
          close(watch_fds[i].fd);
          cache_plugin->connections_.erase(watch_fds[i].fd);
          watch_fds.erase(watch_fds.begin() + i);
          if ( (getenv(CacheTransport::kEnvReadyNotifyFd) != NULL) &&
               (cache_plugin->connections_.empty()) &&
               (cache_plugin->num_inlimbo_clients_ == 0) )
          {
            LogCvmfs(kLogCache, kLogSyslog,
                     "stopping cache plugin, no more active clients");
            terminated = true;
            break;
          }
        } else {
          i++;
        }
      } else {
        i++;
      }
    }
  }

  // 0, 1 being closed by destructor
  for (unsigned i = 2; i < watch_fds.size(); ++i)
    close(watch_fds[i].fd);
  cache_plugin->txn_ids_.Clear();

  signal(SIGPIPE, save_sigpipe);
  return NULL;
}


/**
 * Used during startup to synchronize with the cvmfs client.
 */
void CachePlugin::NotifySupervisor(char signal) {
  char *pipe_ready = getenv(CacheTransport::kEnvReadyNotifyFd);
  if (pipe_ready == NULL)
    return;
  int fd_pipe_ready = String2Int64(pipe_ready);
  WritePipe(fd_pipe_ready, &signal, 1);
}


void CachePlugin::ProcessRequests(unsigned num_workers) {
  num_workers_ = num_workers;
  int retval = pthread_create(&thread_io_, NULL, MainProcessRequests, this);
  assert(retval == 0);
  NotifySupervisor(CacheTransport::kReadyNotification);
  atomic_cas32(&running_, 0, 1);
}


void CachePlugin::SendDetachRequests() {
  set<int>::const_iterator iter = connections_.begin();
  set<int>::const_iterator iter_end = connections_.end();
  for (; iter != iter_end; ++iter) {
    CacheTransport transport(*iter,
      CacheTransport::kFlagSendIgnoreFailure |
      CacheTransport::kFlagSendNonBlocking);
    cvmfs::MsgDetach msg_detach;
    CacheTransport::Frame frame_send(&msg_detach);
    transport.SendFrame(&frame_send);
  }
}


void CachePlugin::Terminate() {
  if (IsRunning()) {
    char terminate = kSignalTerminate;
    WritePipe(pipe_ctrl_[1], &terminate, 1);
    pthread_join(thread_io_, NULL);
    atomic_cas32(&running_, 1, 0);
  }
}


void CachePlugin::WaitFor() {
  if (!IsRunning())
    return;
  pthread_join(thread_io_, NULL);
}
