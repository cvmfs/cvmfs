/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/guardian.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include "nfs4/logging.h"
#include "nfs4/mount_util.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/rpc.h"
#include "nfs4/xdr.h"

namespace nfs4 {

namespace {

const unsigned kUnmountAttempts = 120;
const unsigned kIdleSecondsBeforeExit = 5;

/** Fails every COMPOUND at its first operation */
class ErrorProgram : public RpcProgram {
 public:
  virtual uint32_t program() const { return kNfsProgram; }
  virtual uint32_t min_version() const { return kNfsVersion4; }
  virtual uint32_t max_version() const { return kNfsVersion4; }
  virtual AcceptStat Dispatch(const RpcCall &call, XdrReader *args,
                              XdrWriter *res) {
    if (call.procedure == kNfsProcNull)
      return kAcceptSuccess;
    if (call.procedure != kNfsProcCompound)
      return kAcceptProcUnavail;
    std::string tag;
    uint32_t minor_version = 0, num_ops = 0, first_op = 0;
    args->GetOpaque(&tag, kNfs4OpaqueLimit);
    args->GetU32(&minor_version);
    args->GetU32(&num_ops);
    args->GetU32(&first_op);
    res->PutU32(NFS4ERR_IO);
    res->PutOpaque(tag);
    if (num_ops == 0 || !args->ok()) {
      res->PutU32(0);
      return kAcceptSuccess;
    }
    res->PutU32(1);
    res->PutU32(first_op);
    res->PutU32(NFS4ERR_IO);
    return kAcceptSuccess;
  }
};

}  // anonymous namespace


int RunGuardian(int listen_fd, int control_fd, const std::string &mountpoint) {
  // Started by cvmfs2 with its effective uid; unmounting needs root
  if (getuid() == 0 && geteuid() != 0) {
    if (seteuid(0) == 0)
      setegid(0);
  }
  signal(SIGHUP, SIG_IGN);
  signal(SIGINT, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);

  // Serve mount and unmount requests until cvmfs2 exits (EOF)
  struct sockaddr_in addr;
  socklen_t addr_len = sizeof(addr);
  uint16_t port = 0;
  if (getsockname(listen_fd, reinterpret_cast<struct sockaddr *>(&addr),
                  &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }
  uint64_t mount_id = 0;
  while (true) {
    char command;
    const ssize_t n = read(control_fd, &command, 1);
    if (n < 0 && errno == EINTR)
      continue;
    if (n != 1)
      break;
    bool ok = false;
    if (command == 'M') {
      ok = MountNfs(mountpoint, port);
      if (ok)
        mount_id = FindMount(mountpoint, port);
    } else if (command == 'U') {
      ok = !IsMounted(mountpoint, mount_id) || UnmountNfs(mountpoint);
    }
    const char status = ok ? 0 : 1;
    if (write(control_fd, &status, 1) != 1)
      break;
  }
  close(control_fd);
  // A newer mount on the same path belongs to another cvmfs2
  if (!IsMounted(mountpoint, mount_id))
    return 0;

  Log(kLogLevelError,
      "cvmfs2 serving %s is gone, failing NFS requests until unmounted",
      mountpoint.c_str());
  ErrorProgram program;
  RpcServer::Options options;
  options.workers = 2;
  RpcServer rpc(&program, options);
  if (rpc.Adopt(listen_fd))
    rpc.Start();

  for (unsigned i = 0; i < kUnmountAttempts && IsMounted(mountpoint, mount_id); ++i) {
    if (UnmountNfs(mountpoint))
      break;
    sleep(1);
  }
  if (IsMounted(mountpoint, mount_id)) {
    rpc.Stop();
    Log(kLogLevelError, "failed to unmount %s", mountpoint.c_str());
    return 1;
  }
  Log(kLogLevelInfo, "unmounted %s", mountpoint.c_str());

  // A lazily detached mount (Linux, files still open) keeps its connection
  // and hard-mount requests would hang without an answer; keep failing them
  // until the client is gone
  unsigned idle = 0;
  while (idle < kIdleSecondsBeforeExit) {
    idle = (rpc.NumConnections() == 0) ? idle + 1 : 0;
    sleep(1);
  }
  rpc.Stop();
  return 0;
}


}  // namespace nfs4
