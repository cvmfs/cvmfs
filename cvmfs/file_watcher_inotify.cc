/**
 * This file is part of the CernVM File System.
 */

#include "file_watcher_inotify.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <poll.h>
#include <sys/inotify.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <string>
#include <vector>

#include "util/logging.h"
#include "util/posix.h"

namespace file_watcher {

FileWatcherInotify::FileWatcherInotify() { }

FileWatcherInotify::~FileWatcherInotify() { }

bool FileWatcherInotify::RunEventLoop(const FileWatcher::HandlerMap &handlers,
                                      int read_pipe, int write_pipe) {
  inotify_fd_ = inotify_init1(IN_NONBLOCK);
  assert(inotify_fd_ >= 0);

  for (FileWatcher::HandlerMap::const_iterator it = handlers.begin();
       it != handlers.end();
       ++it) {
    RegisterFilter(it->first, it->second);
  }

  // Use the control pipe to signal readiness to the main thread,
  // before continuing with the event loop.
  WritePipe(write_pipe, "s", 1);

  struct pollfd poll_set[2];

  poll_set[0].fd = read_pipe;
  poll_set[0].events = POLLHUP | POLLIN;
  poll_set[0].revents = 0;
  poll_set[1].fd = inotify_fd_;
  poll_set[1].events = POLLIN;
  poll_set[1].revents = 0;

  bool stop = false;
  while (!stop) {
    int ready = poll(poll_set, 2, -1);
    if (ready == -1) {
      if (errno == EINTR) {
        continue;
      }
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "FileWatcherInotify - Could not poll events. Errno: %d", errno);
      return false;
    }
    if (ready == 0) {
      continue;
    }

    if (poll_set[0].revents & POLLHUP) {
      LogCvmfs(kLogCvmfs, kLogDebug, "FileWatcherInotify - Stopping.\n");
      stop = true;
      continue;
    }
    if (poll_set[0].revents & POLLIN) {
      char buffer[1];
      ReadPipe(read_pipe, &buffer, 1);
      LogCvmfs(kLogCvmfs, kLogDebug, "FileWatcherInotify - Stopping.\n");
      stop = true;
      continue;
    }

    const size_t event_size = sizeof(struct inotify_event);
    // We need a large enough buffer for an event with the largest path name
    const size_t buffer_size = event_size + PATH_MAX + 1;
    char buffer[buffer_size];
    if (poll_set[1].revents & POLLIN) {
      int len = read(inotify_fd_, buffer, buffer_size);
      assert(len > 0);
      int i = 0;
      while (i < len) {
        struct inotify_event
            *inotify_event = reinterpret_cast<struct inotify_event *>(
                &buffer[i]);
        std::map<int, WatchRecord>::const_iterator it = watch_records_.find(
            inotify_event->wd);
        if (it != watch_records_.end()) {
          WatchRecord current_record = it->second;
          file_watcher::Event event = file_watcher::kInvalid;
          if (inotify_event->mask & IN_DELETE_SELF) {
            event = file_watcher::kDeleted;
          } else if (inotify_event->mask & IN_CLOSE_WRITE) {
            // Modified
            event = file_watcher::kModified;
          } else if (inotify_event->mask & IN_MOVE_SELF) {
            // Renamed
            event = file_watcher::kRenamed;
          } else if (inotify_event->mask & IN_ATTRIB) {
            // Attributes
            event = file_watcher::kAttributes;
          } else if (inotify_event->mask & IN_IGNORED) {
            // An IN_IGNORED event is generated after a file is deleted and the
            // watch is removed
            event = file_watcher::kIgnored;
          }
          bool clear_handler = true;
          if (event != file_watcher::kInvalid
              && event != file_watcher::kIgnored) {
            current_record.handler_->Handle(current_record.file_path_, event,
                                            &clear_handler);
          } else {
            LogCvmfs(kLogCvmfs, kLogDebug,
                     "FileWatcherInotify - Unknown event 0x%x\n",
                     inotify_event->mask);
          }

          // Perform post-handling actions (i.e. remove, reset filter)
          if (event == file_watcher::kDeleted) {
            watch_records_.erase(inotify_event->wd);
            if (!clear_handler) {
              RegisterFilter(current_record.file_path_,
                             current_record.handler_);
            }
          }
        } else {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "FileWatcherInotify - Unknown event ident: %d",
                   inotify_event->wd);
        }

        i += event_size + inotify_event->len;
      }
    }
  }

  watch_records_.clear();

  close(inotify_fd_);

  return true;
}

int FileWatcherInotify::TryRegisterFilter(const std::string &file_path) {
  return inotify_add_watch(
      inotify_fd_, file_path.c_str(),
      IN_ATTRIB | IN_CLOSE_WRITE | IN_DELETE_SELF | IN_MOVE_SELF);
}

}  // namespace file_watcher
