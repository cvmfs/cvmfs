/**
 * This file is part of the CernVM File System.
 */


#include "loader_talk.h"

#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>

#include "loader.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"

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
    if (con_fd >= 0) {
      shutdown(con_fd, SHUT_RDWR);
      close(con_fd);
    }
    if ((con_fd = accept(socket_fd_, (struct sockaddr *)&remote, &socket_size))
         < 0)
    {
      break;
    }

    char command;
    ReloadMode reload_mode = kReloadLegacy;
    if (recv(con_fd, &command, 1, 0) > 0) {
      if ((command == 'd') || (command == 'n')) {
        // version that specifies reloading in debug or non-debug mode
        // receives 2 commands
        // first: debug (d) / non-debug(n)
        // second: 'R' or 'S'
        reload_mode = command == 'd' ? kReloadDebug : kReloadNoDebug;
        if (recv(con_fd, &command, 1, 0) > 0) {
          if ((command != 'R') && (command != 'S')) {
            SendMsg2Socket(con_fd, "unknown command\n");
            continue;
          }
        }
      } else if ((command != 'R') && (command != 'S')) {  // legacy support
        SendMsg2Socket(con_fd, "unknown command\n");
        continue;
      }

      SetLogMicroSyslog(*usyslog_path_);
      LogCvmfs(kLogCvmfs, kLogSyslog, "reloading Fuse module. Reload mode=%d",
                                      reload_mode);
      int retval = Reload(con_fd, command == 'S', reload_mode);
      SendMsg2Socket(con_fd, "~");
      (void)send(con_fd, &retval, sizeof(retval), MSG_NOSIGNAL);
      if (retval != kFailOk) {
        PANIC(kLogSyslogErr, "reloading Fuse module failed (%d - %s)", retval,
              Code2Ascii(static_cast<Failures>(retval)));
      }
      SetLogMicroSyslog("");
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
int MainReload(const std::string &socket_path, const bool stop_and_go,
               const bool debug) {
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Connecting to CernVM-FS loader... ");
  int socket_fd = ConnectSocket(socket_path);
  if (socket_fd < 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed!");
    return 100;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  // reload mode: debug (d) or non-debug (n)
  char commands[2];
  commands[0] = debug ? 'd' : 'n';
  commands[1] = stop_and_go ? 'S' : 'R';

  // Loaders before version 2.11 won't recognize 'd' or 'n'. They will
  // send "unknown command" and close the connection. We try to send
  // both the commands and react according to what we read from the socket.

  ssize_t retval;
  do {
    retval = send(socket_fd, commands, 2, MSG_NOSIGNAL);
  } while ((retval <= 0) && (errno == EINTR));

  if (retval <= 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Sending reload command failed!");
    return 103;
  }

  char buf;
  std::string first_line;
  bool past_first_line = false;
  while ((retval = read(socket_fd, &buf, 1)) == 1) {
    if (buf == '~')
      break;

    if (first_line == "unknown command") {
      // We have a pre-2.11 loader, reconnect to the socket
      LogCvmfs(kLogCvmfs, kLogStdout,
               "Connecting in backwards compatibility mode");
      close(socket_fd);
      socket_fd = ConnectSocket(socket_path);
      if (socket_fd < 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "reconnecting failed!");
        return 104;
      }
      WritePipe(socket_fd, &commands[1], 1);
      first_line.clear();
      past_first_line = true;
      continue;
    }

    // chars are received one by one; in order to check if we get
    // "unknown command" a string is constructed here
    if (!past_first_line) {
      if (buf == '\n') {
        LogCvmfs(kLogCvmfs, kLogStdout, "%s", first_line.c_str());
        past_first_line = true;
      } else {
        first_line.push_back(buf);
      }
      continue;
    }

    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%c", buf);
  }
  if (retval != 1) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Reload CRASHED! "
             "CernVM-FS mountpoints unusable.");
    return 101;
  }

  int result = 102;
  if (read(socket_fd, &result, sizeof(result)) < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Socket read FAILED! "
             "CernVM-FS mountpoints unusable.");
  } else {
    if (result != kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Reload FAILED! "
               "CernVM-FS mountpoints unusable.");
    }
  }

  return result;
}

}  // namespace loader_talk
}  // namespace loader
