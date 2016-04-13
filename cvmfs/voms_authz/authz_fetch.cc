/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS
#include "authz_fetch.h"

#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <syslog.h>
#include <unistd.h>

#include <cassert>

#include "clientctx.h"
#include "logging.h"
#include "platform.h"
#include "util_concurrency.h"
#include "util/posix.h"

using namespace std;  // NOLINT

AuthzExternalFetcher::AuthzExternalFetcher(
  const string &fqrn,
  const string &progname)
  : fqrn_(fqrn)
  , progname_(progname)
  , fd_send_(-1)
  , fd_recv_(-1)
  , pid_(-1)
{
  InitLock();
}

AuthzExternalFetcher::AuthzExternalFetcher(
  const string &fqrn,
  int fd_send,
  int fd_recv)
  : fqrn_(fqrn)
  , fd_send_(fd_send)
  , fd_recv_(fd_recv)
  , pid_(-1)
{
  InitLock();
}


AuthzExternalFetcher::~AuthzExternalFetcher() {
  int retval = pthread_mutex_destroy(&lock_);
  assert(retval == 0);

  if (fd_send_ >= 0)
    close(fd_send_);
  if (fd_recv_ >= 0)
    close(fd_recv_);

  if (pid_ > 0) {
    uint64_t now = platform_monotonic_time();
    int statloc;
    do {
      retval = waitpid(pid_, &statloc, WNOHANG);
      if (platform_monotonic_time() > (now + kChildTimeout)) {
        LogCvmfs(kLogAuthz, kLogSyslogWarn | kLogDebug,
                 "authz helper %s unresponsive, killing", progname_.c_str());
        kill(pid_, SIGKILL);
        break;
      }
    } while (retval == 0);
  }
}


/**
 * Uses execve to start progname_.  The started program has stdin and stdout
 * connected to fd_send_ and fd_recv_ and the CVMFS_... environment variables
 * set.  Special care must be taken when we call fork here in an unknown state
 * of the client.  Therefore we can't use ManagedExec (we can't use malloc).
 *
 * A failed execve is not caught by this routine.  It will be caught in the
 * next step, when mother and child start talking.
 */
void AuthzExternalFetcher::ExecHelper() {
  int pipe_send[2];
  int pipe_recv[2];
  MakePipe(pipe_send);
  MakePipe(pipe_recv);
  char *env_fqrn = strdupa(("CVMFS_FQRN=" + fqrn_).c_str());
  char *argv0 = strdupa(progname_.c_str());
  char *argv[] = {argv0, NULL};
  char *envp[] = {env_fqrn, NULL};
  int max_fd = sysconf(_SC_OPEN_MAX);
  assert(max_fd > 0);
  LogCvmfs(kLogAuthz, kLogDebug | kLogSyslog, "starting authz helper %s",
           argv0);

  pid_t pid = fork();
  if (pid == 0) {
    // Child process, close file descriptors and run the helper
    int retval = dup2(pipe_send[0], 0);
    assert(retval == 0);
    retval = dup2(pipe_recv[1], 1);
    assert(retval == 1);
    for (int fd = 2; fd < max_fd; fd++)
      close(fd);

    execve(argv0, argv, envp);
    syslog(LOG_USER | LOG_ERR, "failed to start authz helper %s (%d)",
           argv0, errno);
    abort();
  }
  assert(pid > 0);
  close(pipe_send[0]);
  close(pipe_recv[1]);

  pid_ = pid;
  fd_send_ = pipe_send[1];
  fd_recv_ = pipe_recv[0];
}


AuthzStatus AuthzExternalFetcher::FetchWithinClientCtx(
  const std::string &membership,
  AuthzToken *authz_token,
  unsigned *ttl)
{
  assert(ClientCtx::GetInstance()->IsSet());
  uid_t uid;
  gid_t gid;
  pid_t pid;
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);

  MutexLockGuard lock_guard(lock_);

  if (fd_send_ < 0)
    ExecHelper();
  assert((fd_send_ >= 0) && (fd_recv_ >= 0));


  return kAuthzUnknown;
}


void AuthzExternalFetcher::InitLock() {
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
}
