/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/callback.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#include "nfs4/logging.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/xdr.h"

namespace nfs4 {

namespace {

const uint32_t kCallbackVersion = 1;
const uint32_t kCbProcNull = 0;
const uint32_t kCbProcCompound = 1;
const uint32_t kOpCbRecall = 4;
const uint32_t kLastFragment = 0x80000000u;
const uint32_t kMaxReply = 64 * 1024;

/** Waits until fd is readable or writable, at most timeout_ms */
bool WaitFd(int fd, short events, unsigned timeout_ms) {
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = events;
  pfd.revents = 0;
  int retval;
  do {
    retval = poll(&pfd, 1, static_cast<int>(timeout_ms));
  } while (retval < 0 && errno == EINTR);
  return retval > 0 && !(pfd.revents & (POLLERR | POLLNVAL));
}

bool SendAll(int fd, const char *p, size_t len) {
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

bool RecvAll(int fd, char *p, size_t len, unsigned timeout_ms) {
  while (len > 0) {
    if (!WaitFd(fd, POLLIN, timeout_ms))
      return false;
    const ssize_t n = recv(fd, p, len, 0);
    if (n == 0)
      return false;
    if (n < 0) {
      if (errno == EINTR || errno == EAGAIN)
        continue;
      return false;
    }
    p += n;
    len -= n;
  }
  return true;
}

}  // anonymous namespace


CallbackClient::CallbackClient(uint32_t program, uint32_t ident,
                               const std::string &netid,
                               const std::string &address)
    : program_(program)
    , ident_(ident)
    , port_(0)
    , fd_(-1)
    , next_xid_(static_cast<uint32_t>(time(NULL)) << 8) {
  if (netid != "tcp")
    return;
  // Universal address: a.b.c.d.p1.p2
  const size_t last = address.rfind('.');
  if (last == std::string::npos || last == 0)
    return;
  const size_t second = address.rfind('.', last - 1);
  if (second == std::string::npos)
    return;
  const int hi = atoi(address.substr(second + 1, last - second - 1).c_str());
  const int lo = atoi(address.substr(last + 1).c_str());
  if (hi < 0 || hi > 255 || lo < 0 || lo > 255)
    return;
  host_ = address.substr(0, second);
  struct in_addr probe;
  if (inet_pton(AF_INET, host_.c_str(), &probe) != 1)
    return;
  port_ = static_cast<uint16_t>(hi * 256 + lo);
}


CallbackClient::~CallbackClient() { Disconnect(); }


void CallbackClient::Disconnect() {
  if (fd_ >= 0)
    close(fd_);
  fd_ = -1;
}


bool CallbackClient::Connect(unsigned timeout_ms) {
  if (fd_ >= 0)
    return true;
  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ < 0)
    return false;
#ifdef SO_NOSIGPIPE
  const int on = 1;
  setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);
  inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

  const int flags = fcntl(fd_, F_GETFL);
  fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
  int retval = connect(fd_, reinterpret_cast<struct sockaddr *>(&addr),
                       sizeof(addr));
  if (retval != 0 && errno == EINPROGRESS && WaitFd(fd_, POLLOUT, timeout_ms)) {
    int error = 0;
    socklen_t len = sizeof(error);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len);
    retval = (error == 0) ? 0 : -1;
  }
  if (retval != 0) {
    Log(kLogLevelDebug, "cannot connect to callback %s:%u", host_.c_str(),
        port_);
    Disconnect();
    return false;
  }
  return true;
}


bool CallbackClient::Call(uint32_t procedure, const std::string &args,
                          std::string *result, unsigned timeout_ms) {
  std::lock_guard<std::mutex> const guard(lock_);
  if (!valid())
    return false;

  // A broken connection is reestablished once
  for (unsigned attempt = 0; attempt < 2; ++attempt) {
    if (!Connect(timeout_ms))
      return false;
    const uint32_t xid = next_xid_++;
    XdrWriter w;
    const size_t marker_offset = w.ReserveU32();
    w.PutU32(xid);
    w.PutU32(kRpcCall);
    w.PutU32(kRpcVersion);
    w.PutU32(program_);
    w.PutU32(kCallbackVersion);
    w.PutU32(procedure);
    // AUTH_SYS as root: the Linux client rejects CB_COMPOUND with AUTH_NONE
    XdrWriter cred;
    cred.PutU32(static_cast<uint32_t>(time(NULL)));  // stamp
    cred.PutString("cvmfs");  // machine name
    cred.PutU32(0);  // uid
    cred.PutU32(0);  // gid
    cred.PutU32(0);  // no auxiliary gids
    w.PutU32(kAuthSys);
    w.PutOpaque(cred.buffer());
    w.PutU32(kAuthNone);
    w.PutU32(0);
    w.PutRaw(args.data(), args.size());
    w.PatchU32(marker_offset,
               kLastFragment | static_cast<uint32_t>(w.size() - 4));
    if (!SendAll(fd_, w.buffer().data(), w.size())) {
      Disconnect();
      continue;
    }

    // Read records until the reply to our xid arrives
    while (true) {
      uint32_t marker;
      if (!RecvAll(fd_, reinterpret_cast<char *>(&marker), 4, timeout_ms)) {
        Disconnect();
        return false;
      }
      marker = ntohl(marker);
      const uint32_t len = marker & ~kLastFragment;
      if (len > kMaxReply) {
        Disconnect();
        return false;
      }
      std::string record(len, '\0');
      if (len > 0 && !RecvAll(fd_, &record[0], len, timeout_ms)) {
        Disconnect();
        return false;
      }
      XdrReader r(reinterpret_cast<const unsigned char *>(record.data()),
                  record.size());
      uint32_t reply_xid, type, reply_stat, verf_flavor, accept;
      std::string verf;
      r.GetU32(&reply_xid);
      r.GetU32(&type);
      if (!r.ok() || reply_xid != xid || type != kRpcReply)
        continue;
      r.GetU32(&reply_stat);
      r.GetU32(&verf_flavor);
      r.GetOpaque(&verf, 400);
      r.GetU32(&accept);
      if (!r.ok() || reply_stat != kMsgAccepted || accept != kAcceptSuccess)
        return false;
      result->assign(reinterpret_cast<const char *>(r.cursor()),
                     r.remaining());
      return true;
    }
  }
  return false;
}


bool CallbackClient::Null(unsigned timeout_ms) {
  std::string result;
  return Call(kCbProcNull, "", &result, timeout_ms);
}


int CallbackClient::Recall(uint32_t stateid_seqid,
                           const std::string &stateid_other,
                           const std::string &fh, unsigned timeout_ms) {
  XdrWriter args;
  args.PutString("");  // tag
  args.PutU32(0);  // minor version
  args.PutU32(ident_);
  args.PutU32(1);  // one op
  args.PutU32(kOpCbRecall);
  args.PutU32(stateid_seqid);
  args.PutFixedOpaque(stateid_other.data(), kStateidOtherSize);
  args.PutBool(false);  // truncate
  args.PutOpaque(fh);
  std::string result;
  if (!Call(kCbProcCompound, args.buffer(), &result, timeout_ms))
    return -1;
  XdrReader r(reinterpret_cast<const unsigned char *>(result.data()),
              result.size());
  uint32_t status;
  if (!r.GetU32(&status))
    return -1;
  return static_cast<int>(status);
}

}  // namespace nfs4
