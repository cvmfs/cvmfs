/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/rpc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "nfs4/logging.h"

namespace nfs4 {

namespace {

const uint32_t kMaxAuthBody = 400;
const uint32_t kMaxAuthSysGids = 16;
const uint32_t kLastFragment = 0x80000000u;

void PutReplyHeader(uint32_t xid, XdrWriter *w) {
  w->PutU32(xid);
  w->PutU32(kRpcReply);
}

void PutAccepted(uint32_t xid, AcceptStat stat, XdrWriter *w) {
  PutReplyHeader(xid, w);
  w->PutU32(kMsgAccepted);
  w->PutU32(kAuthNone);  // verifier
  w->PutU32(0);
  w->PutU32(stat);
}

bool ParseAuthSys(const std::string &body, Credentials *creds) {
  XdrReader r(reinterpret_cast<const unsigned char *>(body.data()),
              body.size());
  uint32_t stamp, ngids;
  std::string machine;
  r.GetU32(&stamp);
  r.GetString(&machine, 255);
  r.GetU32(&creds->uid);
  r.GetU32(&creds->gid);
  r.GetU32(&ngids);
  if (!r.ok() || ngids > kMaxAuthSysGids)
    return false;
  creds->gids.resize(ngids);
  for (uint32_t i = 0; i < ngids; ++i)
    r.GetU32(&creds->gids[i]);
  return r.ok();
}

bool ReadFull(int fd, void *buf, size_t len) {
  char *p = static_cast<char *>(buf);
  while (len > 0) {
    const ssize_t n = read(fd, p, len);
    if (n == 0)
      return false;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    p += n;
    len -= n;
  }
  return true;
}

bool WriteFull(int fd, const char *p, size_t len) {
  int flags = 0;
#ifdef MSG_NOSIGNAL
  flags = MSG_NOSIGNAL;
#endif
  while (len > 0) {
    const ssize_t n = send(fd, p, len, flags);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    p += n;
    len -= n;
  }
  return true;
}

bool IsLoopback(const struct sockaddr_storage &addr) {
  if (addr.ss_family == AF_INET) {
    const struct sockaddr_in *in = reinterpret_cast<const struct sockaddr_in *>(
        &addr);
    return (ntohl(in->sin_addr.s_addr) >> 24) == 127;
  }
  if (addr.ss_family == AF_INET6) {
    const struct sockaddr_in6
        *in6 = reinterpret_cast<const struct sockaddr_in6 *>(&addr);
    return IN6_IS_ADDR_LOOPBACK(&in6->sin6_addr);
  }
  return false;
}

}  // anonymous namespace


bool HandleRpcRecord(RpcProgram *program, const std::string &record,
                     std::string *reply) {
  XdrReader r(reinterpret_cast<const unsigned char *>(record.data()),
              record.size());
  RpcCall call;
  uint32_t msg_type, rpc_version;
  if (!r.GetU32(&call.xid) || !r.GetU32(&msg_type) || msg_type != kRpcCall)
    return false;

  XdrWriter w;
  r.GetU32(&rpc_version);
  r.GetU32(&call.program);
  r.GetU32(&call.version);
  r.GetU32(&call.procedure);
  std::string cred_body, verf_body;
  uint32_t verf_flavor;
  r.GetU32(&call.cred_flavor);
  r.GetOpaque(&cred_body, kMaxAuthBody);
  r.GetU32(&verf_flavor);
  r.GetOpaque(&verf_body, kMaxAuthBody);
  if (!r.ok()) {
    Log(kLogLevelWarn, "garbage RPC header (xid %u)", call.xid);
    PutAccepted(call.xid, kAcceptGarbageArgs, &w);
    reply->swap(*w.mutable_buffer());
    return true;
  }

  if (rpc_version != kRpcVersion) {
    PutReplyHeader(call.xid, &w);
    w.PutU32(kMsgDenied);
    w.PutU32(kRpcMismatch);
    w.PutU32(kRpcVersion);
    w.PutU32(kRpcVersion);
    reply->swap(*w.mutable_buffer());
    return true;
  }

  bool auth_ok = false;
  if (call.cred_flavor == kAuthSys)
    auth_ok = ParseAuthSys(cred_body, &call.creds);
  else if (call.cred_flavor == kAuthNone)
    auth_ok = true;
  if (!auth_ok) {
    PutReplyHeader(call.xid, &w);
    w.PutU32(kMsgDenied);
    w.PutU32(kAuthError);
    w.PutU32(call.cred_flavor == kAuthSys ? kAuthBadCred : kAuthTooWeak);
    reply->swap(*w.mutable_buffer());
    return true;
  }

  if (call.program != program->program()) {
    Log(kLogLevelWarn, "call for unknown program %u (xid %u)", call.program,
        call.xid);
    PutAccepted(call.xid, kAcceptProgUnavail, &w);
    reply->swap(*w.mutable_buffer());
    return true;
  }
  if (call.version < program->min_version()
      || call.version > program->max_version()) {
    PutAccepted(call.xid, kAcceptProgMismatch, &w);
    w.PutU32(program->min_version());
    w.PutU32(program->max_version());
    reply->swap(*w.mutable_buffer());
    return true;
  }

  Log(kLogLevelDebug, "call xid %u proc %u", call.xid, call.procedure);
  call.args = r.cursor();
  call.args_size = r.remaining();
  XdrReader args(call.args, call.args_size);
  XdrWriter results;
  const AcceptStat stat = program->Dispatch(call, &args, &results);
  PutAccepted(call.xid, stat, &w);
  if (stat == kAcceptSuccess)
    w.PutRaw(results.buffer().data(), results.size());
  reply->swap(*w.mutable_buffer());
  return true;
}


RpcServer::Connection::~Connection() { close(fd); }


RpcServer::RpcServer(RpcProgram *program, const Options &options)
    : program_(program)
    , options_(options)
    , listen_fd_(-1)
    , port_(0)
    , stopping_(false)
    , active_readers_(0) { }


RpcServer::~RpcServer() {
  Stop();
  if (listen_fd_ >= 0)
    close(listen_fd_);
}


bool RpcServer::Listen(const std::string &address, uint16_t port) {
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) != 1) {
    Log(kLogLevelError, "invalid listen address %s",
             address.c_str());
    return false;
  }

  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0)
    return false;
  const int on = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr))
          != 0
      || listen(listen_fd_, 64) != 0) {
    Log(kLogLevelError, "failed to listen on %s:%u (%d)", address.c_str(), port, errno);
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  socklen_t len = sizeof(addr);
  getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), &len);
  port_ = ntohs(addr.sin_port);
  return true;
}


bool RpcServer::Adopt(int listen_fd) {
  struct sockaddr_in addr;
  socklen_t len = sizeof(addr);
  if (getsockname(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), &len)
      != 0) {
    return false;
  }
  listen_fd_ = listen_fd;
  port_ = ntohs(addr.sin_port);
  return true;
}


void RpcServer::Start() {
  stopping_ = false;
  for (unsigned i = 0; i < options_.workers; ++i)
    workers_.push_back(std::thread(&RpcServer::WorkerLoop, this));
  accept_thread_ = std::thread(&RpcServer::AcceptLoop, this);
}


void RpcServer::Stop() {
  if (stopping_.exchange(true))
    return;
  if (accept_thread_.joinable())
    accept_thread_.join();

  {
    std::unique_lock<std::mutex> lock(connections_lock_);
    for (unsigned i = 0; i < connections_.size(); ++i) {
      ConnectionPtr c = connections_[i].lock();
      if (c)
        shutdown(c->fd, SHUT_RDWR);
    }
    readers_cond_.wait(lock, [this] { return active_readers_ == 0; });
    connections_.clear();
  }

  queue_cond_.notify_all();
  for (unsigned i = 0; i < workers_.size(); ++i)
    workers_[i].join();
  workers_.clear();
}


void RpcServer::AcceptLoop() {
  while (!stopping_) {
    struct pollfd pfd;
    pfd.fd = listen_fd_;
    pfd.events = POLLIN;
    pfd.revents = 0;
    const int retval = poll(&pfd, 1, 200);
    if (retval <= 0)
      continue;

    struct sockaddr_storage peer;
    socklen_t peer_len = sizeof(peer);
    const int fd = accept(
        listen_fd_, reinterpret_cast<struct sockaddr *>(&peer), &peer_len);
    if (fd < 0)
      continue;
    if (!options_.allow_remote && !IsLoopback(peer)) {
      Log(kLogLevelWarn, "rejected NFS connection from non-loopback peer");
      close(fd);
      continue;
    }
    const int on = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#ifdef SO_NOSIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif

    ConnectionPtr connection(new Connection(fd));
    std::lock_guard<std::mutex> lock(connections_lock_);
    // Drop entries of closed connections
    std::vector<std::weak_ptr<Connection> > alive;
    for (unsigned i = 0; i < connections_.size(); ++i) {
      if (!connections_[i].expired())
        alive.push_back(connections_[i]);
    }
    alive.push_back(connection);
    connections_.swap(alive);
    active_readers_++;
    std::thread(&RpcServer::ReadLoop, this, connection).detach();
  }
}


void RpcServer::ReadLoop(ConnectionPtr connection) {
  std::string record;
  while (!stopping_) {
    uint32_t marker;
    if (!ReadFull(connection->fd, &marker, sizeof(marker)))
      break;
    marker = ntohl(marker);
    const uint32_t len = marker & ~kLastFragment;
    if (record.size() + len > options_.max_record) {
      Log(kLogLevelWarn, "RPC record exceeds %u bytes, closing connection",
               options_.max_record);
      break;
    }
    const size_t offset = record.size();
    record.resize(offset + len);
    if (len > 0 && !ReadFull(connection->fd, &record[offset], len))
      break;
    if (!(marker & kLastFragment))
      continue;

    Job job;
    job.connection = connection;
    job.xid = 0;
    if (record.size() >= 4) {
      memcpy(&job.xid, record.data(), 4);
      job.xid = ntohl(job.xid);
    }
    Log(kLogLevelDebug, "recv xid %u (%zu bytes)", job.xid, record.size());
    {
      // A retransmission of a call that is still being processed is
      // answered by the original
      std::lock_guard<std::mutex> lock(connection->inflight_lock);
      if (!connection->inflight.insert(job.xid).second) {
        Log(kLogLevelDebug, "dropping retransmission of xid %u", job.xid);
        record.clear();
        continue;
      }
    }
    job.record.swap(record);
    {
      std::lock_guard<std::mutex> lock(queue_lock_);
      queue_.push_back(Job());
      queue_.back().connection.swap(job.connection);
      queue_.back().xid = job.xid;
      queue_.back().record.swap(job.record);
    }
    queue_cond_.notify_one();
  }

  shutdown(connection->fd, SHUT_RDWR);
  std::lock_guard<std::mutex> lock(connections_lock_);
  active_readers_--;
  readers_cond_.notify_all();
}


void RpcServer::WorkerLoop() {
  while (true) {
    Job job;
    {
      std::unique_lock<std::mutex> lock(queue_lock_);
      queue_cond_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty())
        return;
      job.connection.swap(queue_.front().connection);
      job.xid = queue_.front().xid;
      job.record.swap(queue_.front().record);
      queue_.pop_front();
    }

    std::string reply;
    if (HandleRpcRecord(program_, job.record, &reply)) {
      Send(job.connection, reply);
      Log(kLogLevelDebug, "sent xid %u (%zu bytes)", job.xid, reply.size());
    } else {
      Log(kLogLevelWarn, "ignoring record that is not a call (xid %u)",
          job.xid);
    }
    std::lock_guard<std::mutex> lock(job.connection->inflight_lock);
    job.connection->inflight.erase(job.xid);
  }
}


void RpcServer::Send(const ConnectionPtr &connection,
                     const std::string &reply) {
  const uint32_t marker = htonl(kLastFragment
                                | static_cast<uint32_t>(reply.size()));
  std::lock_guard<std::mutex> lock(connection->write_lock);
  if (!WriteFull(connection->fd, reinterpret_cast<const char *>(&marker),
                 sizeof(marker))
      || !WriteFull(connection->fd, reply.data(), reply.size())) {
    Log(kLogLevelWarn, "failed to send reply (%d), closing connection", errno);
    shutdown(connection->fd, SHUT_RDWR);
  }
}

}  // namespace nfs4
