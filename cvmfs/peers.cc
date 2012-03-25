/**
 * This file is part of the CernVM File System.
 *
 * The peer server is a shared daemon among all currently mounted file system
 * instances (repository).  It is automatically created by the first instance
 * and automatically shuts itself done when the last instance is unmounted.
 *
 * The peer server maintains a list of available cvmfs peers.  Neighbor cvmfs
 * instances are discovered by IP multicast.  Availability of cvmfs instances
 * is supervised by watchdogs.
 *
 * Cvmfs instances ask the peer server via sockets for a responsible peer
 * given a certain hash.  The peers server implements a distributed hash table.
 *
 * This module contains both server and client code.
 */

#include "peers.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include "util.h"
#include "logging.h"
#include "atomic.h"
#include "smalloc.h"
#include "cvmfs.h"

using namespace std;  // NOLINT

namespace peers {

// Server variables
std::string *cachedir_ = NULL;
atomic_int32 num_connections_;
pthread_attr_t pthread_connection_attr_;

// Client variables
int socket_fd_ = -1;


/**
 * Connects to a running peer server.  Creates a peer server, if necessary.
 */
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
  socket_fd_ = ConnectSocket(*cachedir_ + "/peers");
  if (socket_fd_ != -1) {
    char buf = '\0';
    read(socket_fd_, &buf, 1);
    if (buf == 'C') {
      LogCvmfs(kLogPeers, kLogDebug, "connected to existing socket");
      UnlockFile(fd_lockfile);
      return true;
    }
  }

  // Opening new socket for peer server (to be created)
  int socket_pair[2];
  int retval = socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair);
  assert(retval == 0);
  socket_fd_ = socket_pair[0];

  // Create new peer server
  int pipe_fork[2];
  int pipe_boot[2];
  MakePipe(pipe_fork);
  MakePipe(pipe_boot);

  pid_t pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    // Double fork to disconnect from parent
    pid_t pid_peer_server = fork();
    assert(pid_peer_server >= 0);
    if (pid_peer_server != 0) _exit(0);

    int max_fd;
    int fd_flags;
    const char *argv[] = {exe_path.c_str(), "__peersrv__", cachedir_->c_str(),
                          StringifyInt(pipe_boot[1]).c_str(),
                          StringifyInt(socket_pair[1]).c_str(),
                          StringifyInt(cvmfs::foreground_).c_str(),
                          GetLogDebugFile().c_str(), NULL};
    char failed = 'U';

    // Child, close file descriptors
    max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0) {
      failed = 'C';
      goto fork_failure;
    }
    for (int fd = 3; fd < max_fd; fd++) {
      if ((fd != pipe_fork[1]) && (fd != pipe_boot[1]) &&
          (fd != socket_pair[1]))
      {
        close(fd);
      }
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

    execvp(exe_path.c_str(), const_cast<char **>(argv));

    failed = 'E';

   fork_failure:
    write(pipe_fork[1], &failed, 1);
    _exit(1);
  }
  int statloc;
  waitpid(pid, &statloc, 0);

  close(socket_pair[1]);
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

  LogCvmfs(kLogPeers, kLogDebug, "disconnecting from peer server");
  if (socket_fd_ >= 0) {
    close(socket_fd_);
  }
}


/**
 * A connection to a mounted repository, receives hashes and returns the
 * repsonsible peer (redis) address.
 */
static void *MainPeerConnection(void *data) {
  int connection_fd = *(reinterpret_cast<int *>(data));
  free(data);
  LogCvmfs(kLogPeers, kLogDebug, "starting new peer connection on %d",
           connection_fd);

  char buf;
  read(connection_fd, &buf, 1);

  LogCvmfs(kLogPeers, kLogDebug, "shutting down peer connection %d",
           connection_fd);
  close(connection_fd);

  // Clean up after last instance
  int active_connections = atomic_xadd32(&num_connections_, -1);
  if (active_connections == 1) {
    LogCvmfs(kLogPeers, kLogDebug, "last connection, stopping peer server");
    exit(0);
  }

  return NULL;
}


/**
 * Accepts connection requests from new file system instances (new mounted
 * repositories).  The spawning instance is dealt with differenlty: here the
 * socket is a pre-created anonymous socketpair.
 */
int MainPeerServer(int argc, char **argv) {
  LogCvmfs(kLogPeers, kLogDebug, "starting peer server");
  int retval;

  // Process command line arguments
  cachedir_ = new string(argv[2]);
  int pipe_boot = String2Int64(argv[3]);
  int inital_socket = String2Int64(argv[4]);
  int foreground = String2Int64(argv[5]);
  const string logfile = argv[6];
  if (logfile != "")
    SetLogDebugFile(logfile + ".peersrv");

  retval = pthread_attr_init(&pthread_connection_attr_);
  assert(retval == 0);
  retval = pthread_attr_setdetachstate(&pthread_connection_attr_,
                                       PTHREAD_CREATE_DETACHED);
  assert(retval == 0);
  pthread_t pthread_connection;

  int socket_fd = MakeSocket(*cachedir_ + "/peers", 0600);
  if (socket_fd == -1) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to create peer socket (%d)", errno);
    return 1;
  }
  if (listen(socket_fd, 128) != 0) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to listen at peer socket (%d)",
             errno);
    return 1;
  }
  LogCvmfs(kLogPeers, kLogDebug, "listening on %s",
           (*cachedir_ + "/peers").c_str());

  if (!foreground) {
    retval = daemon(1, 0);
    assert(retval == 0);
  }
  char buf = 'C';
  WritePipe(pipe_boot, &buf, 1);
  close(pipe_boot);

  atomic_init32(&num_connections_);
  atomic_inc32(&num_connections_);
  int *connection_fd_ptr =reinterpret_cast<int *>(smalloc(sizeof(int)));
  *connection_fd_ptr = inital_socket;
  retval = pthread_create(&pthread_connection, &pthread_connection_attr_,
                          MainPeerConnection, connection_fd_ptr);
  assert(retval == 0);

  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int connection_fd = -1;
  while (true) {
    connection_fd = accept(socket_fd, (struct sockaddr *)&remote, &socket_size);
    int active_connections = atomic_xadd32(&num_connections_, 1);
    if (active_connections == 0) {
      close(connection_fd);
      continue;  // Will be cleaned up by last thread
    }
    char buf = 'C';
    retval = write(connection_fd, &buf, 1);
    if (retval == 1) {
      connection_fd_ptr =reinterpret_cast<int *>(smalloc(sizeof(int)));
      *connection_fd_ptr = connection_fd;
      retval = pthread_create(&pthread_connection, &pthread_connection_attr_,
                              MainPeerConnection, connection_fd_ptr);
      assert(retval == 0);
    }
  }

  return 0;
}

}  // namespace peers
