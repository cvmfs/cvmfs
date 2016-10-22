/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "channel.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <vector>

#include "logging.h"
#include "util/pointer.h"
#include "util/posix.h"

using namespace std;  // NOLINT

const uint64_t CachePlugin::kSizeUnknown = uint64_t(-1);


void CachePlugin::AskToDetach() {
  char detach = kSignalDetach;
  WritePipe(pipe_ctrl_[1], &detach, 1);
}


CachePlugin::CachePlugin(uint64_t capabilities)
  : capabilities_(capabilities)
  , socket_path_()
  , fd_socket_(-1)
  , running_(false)
  , max_object_size_(kDefaultMaxObjectSize)
{
  atomic_init64(&next_session_id_);
  atomic_init64(&next_txn_id_);
  txn_ids_.Init(128, UniqueRequest(), HashUniqueRequest);
  MakePipe(pipe_ctrl_);
}


CachePlugin::~CachePlugin() {
  if (running_) {
    char terminate = kSignalTerminate;
    WritePipe(pipe_ctrl_[1], &terminate, 1);
    pthread_join(thread_io_, NULL);
  }
  ClosePipe(pipe_ctrl_);
  if (fd_socket_ >= 0)
    close(fd_socket_);
}


void CachePlugin::HandleHandshake(CacheTransport *transport) {
  cvmfs::MsgHandshakeAck msg_ack;
  CacheTransport::Frame frame_send(&msg_ack);

  msg_ack.set_status(cvmfs::STATUS_OK);
  msg_ack.set_name(name_);
  msg_ack.set_protocol_version(kPbProtocolVersion);
  msg_ack.set_max_object_size(max_object_size_);
  msg_ack.set_session_id(NextSessionId());
  msg_ack.set_capabilities(capabilities_);
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleObjectInfo(
  cvmfs::MsgObjectInfoReq *msg_req,
  CacheTransport *transport)
{
  cvmfs::MsgObjectInfoReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval) {
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    ObjectInfo info;
    cvmfs::EnumStatus status = GetObjectInfo(object_id, &info);
    msg_reply.set_status(status);
    if (status == cvmfs::STATUS_OK) {
      msg_reply.set_object_type(info.object_type);
      msg_reply.set_size(info.size);
    }
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleRead(
  cvmfs::MsgReadReq *msg_req,
  CacheTransport *transport)
{
  cvmfs::MsgReadReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval || (msg_req->size() > max_object_size_)) {
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    transport->SendFrame(&frame_send);
    return;
  }
  unsigned size = msg_req->size();
  unsigned char buffer[size];
  cvmfs::EnumStatus status = Pread(object_id, msg_req->offset(), &size, buffer);
  msg_reply.set_status(status);
  if (status == cvmfs::STATUS_OK)
    frame_send.set_attachment(buffer, size);
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleRefcount(
  cvmfs::MsgRefcountReq *msg_req,
  CacheTransport *transport)
{
  cvmfs::MsgRefcountReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);

  msg_reply.set_req_id(msg_req->req_id());
  shash::Any object_id;
  bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
  if (!retval) {
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    cvmfs::EnumStatus status = ChangeRefcount(object_id, msg_req->change_by());
    msg_reply.set_status(status);
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
             "failed to reveive request from connection (%d)", errno);
    return false;
  }

  google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();

  if (msg_typed->GetTypeName() == "cvmfs.MsgHandshake") {
    HandleHandshake(&transport);
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
    return false;
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
  } else {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "unexpected message from client: %s",
             msg_typed->GetTypeName().c_str());
    return false;
  }

  return true;
}


void CachePlugin::HandleStoreAbort(
  cvmfs::MsgStoreAbortReq *msg_req,
  CacheTransport *transport)
{
  cvmfs::MsgStoreReply msg_reply;
  CacheTransport::Frame frame_send(&msg_reply);
  msg_reply.set_req_id(msg_req->req_id());
  msg_reply.set_part_nr(0);
  uint64_t txn_id;
  UniqueRequest uniq_req(msg_req->session_id(), msg_req->req_id());
  bool retval = txn_ids_.Lookup(uniq_req, &txn_id);
  if (!retval) {
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
  } else {
    cvmfs::EnumStatus status = AbortTxn(txn_id);
    msg_reply.set_status(status);
    txn_ids_.Erase(uniq_req);
  }
  transport->SendFrame(&frame_send);
}


void CachePlugin::HandleStore(
  cvmfs::MsgStoreReq *msg_req,
  CacheTransport::Frame *frame,
  CacheTransport *transport)
{
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
    msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    transport->SendFrame(&frame_send);
    return;
  }

  UniqueRequest uniq_req(msg_req->session_id(), msg_req->req_id());
  uint64_t txn_id;
  cvmfs::EnumStatus status = cvmfs::STATUS_OK;
  if (msg_req->part_nr() == 1) {
    if (txn_ids_.Contains(uniq_req)) {
      LogCvmfs(kLogCache, kLogSyslogWarn | kLogDebug,
               "invalid attempt to restart running transaction");
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
      transport->SendFrame(&frame_send);
      return;
    }
    txn_id = NextTxnId();
    ObjectInfo info;
    if (msg_req->has_expected_size()) {info.size = msg_req->expected_size();}
    if (msg_req->has_object_type()) {info.object_type = msg_req->object_type();}
    if (msg_req->has_description()) {info.description = msg_req->description();}
    status = StartTxn(object_id, txn_id, info);
    if (status != cvmfs::STATUS_OK) {
      msg_reply.set_status(status);
      transport->SendFrame(&frame_send);
      return;
    }
    txn_ids_.Insert(uniq_req, txn_id);
  } else {
    retval = txn_ids_.Lookup(uniq_req, &txn_id);
    if (!retval) {
      LogCvmfs(kLogCache, kLogSyslogWarn | kLogDebug, "transaction not found");
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
      msg_reply.set_status(status);
      transport->SendFrame(&frame_send);
      return;
    }
  }

  if (msg_req->last_part()) {
    status = CommitTxn(txn_id);
    txn_ids_.Erase(uniq_req);
  }
  msg_reply.set_status(status);
  transport->SendFrame(&frame_send);
}


bool CachePlugin::Listen(const string &socket_path) {
  socket_path_ = socket_path;
  fd_socket_ = MakeSocket(socket_path_, 0600);
  if (fd_socket_ < 0) {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "failed to create socket %s (%d)", socket_path_.c_str(), errno);
    return false;
  }
  int retval = listen(fd_socket_, 32);
  assert(retval == 0);

  return true;
}


void *CachePlugin::MainProcessRequests(void *data) {
  CachePlugin *cache_plugin = reinterpret_cast<CachePlugin *>(data);

  sighandler_t save_sigpipe = signal(SIGPIPE, SIG_IGN);

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

  while (true) {
    for (unsigned i = 0; i < watch_fds.size(); ++i)
      watch_fds[i].revents = 0;
    int retval = poll(watch_fds.data(), watch_fds.size(), -1);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
               "cache plugin connection failure (%d)", errno);
      abort();
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
    for (unsigned i = 2; i < watch_fds.size(); ++i) {
      if (watch_fds[i].revents) {
        bool proceed = cache_plugin->HandleRequest(watch_fds[i].fd);
        if (!proceed) {
          close(watch_fds[i].fd);
          watch_fds.erase(watch_fds.begin() + i);
          cache_plugin->connections_.erase(watch_fds[i].fd);
        }
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


void CachePlugin::ProcessRequests() {
  int retval = pthread_create(&thread_io_, NULL, MainProcessRequests, this);
  assert(retval == 0);
  running_ = true;
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
