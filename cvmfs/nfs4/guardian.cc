/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/guardian.h"

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include "nfs4/logging.h"
#include "nfs4/mount_util.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/rpc.h"
#include "nfs4/xdr.h"

namespace nfs4 {

namespace {

const unsigned kUnmountAttempts = 120;

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
  signal(SIGHUP, SIG_IGN);
  signal(SIGINT, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);

  // Nothing is ever written; the pipe only signals the peer's exit
  char c;
  ssize_t n;
  do {
    n = read(control_fd, &c, 1);
  } while (n > 0 || (n < 0 && errno == EINTR));
  close(control_fd);
  if (!IsMounted(mountpoint))
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

  for (unsigned i = 0; i < kUnmountAttempts && IsMounted(mountpoint); ++i) {
    if (UnmountNfs(mountpoint))
      break;
    sleep(1);
  }
  rpc.Stop();
  if (IsMounted(mountpoint)) {
    Log(kLogLevelError, "failed to unmount %s", mountpoint.c_str());
    return 1;
  }
  Log(kLogLevelInfo, "unmounted %s", mountpoint.c_str());
  return 0;
}

}  // namespace nfs4
