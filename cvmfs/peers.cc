/**
 * This file is part of the CernVM File System.
 */

#include "peers.h"

#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include "util.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace peers {

std::string *cachedir_ = NULL;

bool Init(const string &cachedir, const string &exe_path) {
  cachedir_ = new string(cachedir);

  // Create lock file
  const int fd_lockfile = LockFile(*cachedir_ + "/lock_peers");
  if (fd_lockfile < 0) {
    LogCvmfs(kLogPeers, kLogDebug, "could not open lock file %s (%d)",
             (*cachedir_ + "/lock_peers").c_str(), errno);
    return false;
  }

  // Try to connect to socket
  LogCvmfs(kLogPeers, kLogDebug, "trying to connect to existing socket");

  // Create new peer server
  int pipe_fork[2];
  int pipe_boot[2];
  MakePipe(pipe_fork);
  MakePipe(pipe_boot);

  pid_t pid_peer_server = fork();
  assert(pid_peer_server >= 0);

  if (pid_peer_server == 0) {
    int max_fd;
    int fd_flags;
    const char *argv[] = {exe_path.c_str(), "__peersrv__", cachedir_->c_str(),
                          StringifyInt(pipe_boot[1]).c_str(), NULL};
    char failed = 'U';

    // Child, close file descriptors
    max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0) {
      failed = 'C';
      goto fork_failure;
    }
    for (int fd = 3; fd < max_fd; fd++) {
      if ((fd != pipe_fork[1]) && (fd != pipe_boot[1]))
        close(fd);
    }

    fd_flags = fcntl(pipe_fork[1], F_GETFD);
    if (fd_flags < 0) {
      failed = 'G';
      goto fork_failure;
    }
    fd_flags |= FD_CLOEXEC;
    if (fcntl(pipe_fork[1], F_SETFD, fd_flags) < 0) {
      failed = 'S';
      goto fork_failure;
    }

    execve(exe_path.c_str(), const_cast<char **>(argv), environ);

    failed = 'E';

   fork_failure:
    write(pipe_fork[1], &failed, 1);
    _exit(1);
  }

  close(pipe_fork[1]);
  char buf;
  if (read(pipe_fork[0], &buf, 1) == 1) {
    UnlockFile(fd_lockfile);
    close(pipe_fork[0]);
    close(pipe_boot[1]);
    close(pipe_boot[0]);
    LogCvmfs(kLogPeers, kLogDebug, "failed to start peer server (%c)", buf);
    return false;
  }
  close(pipe_fork[0]);

  // Wait for peer server to be ready
  close(pipe_boot[1]);
  if (read(pipe_boot[0], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_boot[0]);
    LogCvmfs(kLogPeers, kLogDebug, "peer server did not start");
    return false;
  }
  close(pipe_boot[0]);


  UnlockFile(fd_lockfile);
  return true;
}


void Fini() {
  delete cachedir_;
  cachedir_ = NULL;
}


int MainPeerServer(int argc, char **argv) {
  LogCvmfs(kLogPeers, kLogDebug, "starting peer server");
  cachedir_ = new string(argv[2]);
  int pipe_boot = String2Int64(argv[3]);

  if (!freopen("/dev/null", "w", stdout) ||
      !freopen("/dev/null", "w", stderr) ||
      !freopen("/dev/null", "r", stdin))
  {
    LogCvmfs(kLogPeers, kLogDebug, "failed to disconnect from TTY");
    return 1;
  }

  char buf = 'C';
  WritePipe(pipe_boot, &buf, 1);
  close(pipe_boot);
  return 0;
}

}  // namespace peers
