/**
 * This file is part of the CernVM File System.
 */

#include "loader_talk.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <errno.h>

#include <cstdlib>
#include <cassert>

#include "logging.h"
#include "util.h"
#include "loader.h"

using namespace std;  // NOLINT

namespace loader {
namespace loader_talk {

bool spawned_ = false;
string *socket_path_ = NULL;
int socket_fd_ = -1;
pthread_t thread_talk_;

bool Init(const string &socket_path) {
  spawned_ = false;
  socket_path_ = new string(socket_path);

  socket_fd_ = MakeSocket(*socket_path_, 0600);
  if (socket_fd_ == -1)
    return false;
  if (listen(socket_fd_, 1) == -1) {
    LogCvmfs(kLogCvmfs, kLogDebug, "listening on socket failed (%d)", errno);
    return false;
  }

  unlink((socket_path + ".paused.crashed").c_str());
  unlink((socket_path + ".paused").c_str());

  return true;
}


static void *MainTalk(void *data __attribute__((unused))) {
  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int con_fd = -1;
  while (true) {
    if (con_fd > 0) {
      shutdown(con_fd, SHUT_RDWR);
      close(con_fd);
    }
    if ((con_fd = accept(socket_fd_, (struct sockaddr *)&remote, &socket_size))
         < 0)
    {
      break;
    }

    char command;
    if (recv(con_fd, &command, 1, 0) > 0) {
      if ((command != 'R') && (command != 'S')) {
        SendMsg2Socket(con_fd, "unknown command\n");
        continue;
      }

      LogCvmfs(kLogCvmfs, kLogSyslog, "reloading Fuse module");
      int retval = Reload(con_fd, command == 'S');
      SendMsg2Socket(con_fd, "~");
      (void)send(con_fd, &retval, sizeof(retval), MSG_NOSIGNAL);
      if (retval != kFailOk) {
        LogCvmfs(kLogCvmfs, kLogSyslog, "reloading Fuse module failed (%d)",
                 retval);
        abort();
      }
    }
  }

  return NULL;
}


void Spawn() {
  int retval;
  retval = pthread_create(&thread_talk_, NULL, MainTalk, NULL);
  assert(retval == 0);
  spawned_ = true;
}


void Fini() {
  unlink(socket_path_->c_str());
  shutdown(socket_fd_, SHUT_RDWR);
  close(socket_fd_);
  if (spawned_) pthread_join(thread_talk_, NULL);

  delete socket_path_;
  socket_path_ = NULL;
  spawned_ = false;
  socket_fd_ = -1;
}


/**
 * Connects to a loader socket and triggers the reload
 */
int MainReload(const std::string &socket_path, const bool stop_and_go) {
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Connecting to CernVM-FS loader... ");
  int socket_fd = ConnectSocket(socket_path);
  if (socket_fd < 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed!");
    return 100;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  const char command = stop_and_go ? 'S' : 'R';
  WritePipe(socket_fd, &command, 1);
  char buf;
  int retval;
  while ((retval = read(socket_fd, &buf, 1)) == 1) {
    if (buf == '~')
      break;
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%c", buf);
  }
  if (retval != 1) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Reload CRASHED! "
             "CernVM-FS mountpoints unusuable.");
    return 101;
  }

  int result = 102;
  read(socket_fd, &result, sizeof(result));
  if (result != kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Reload FAILED! "
             "CernVM-FS mountpoints unusuable.");
  }

  return result;
}

}  // namespace loader_talk
}  // namespace loader
