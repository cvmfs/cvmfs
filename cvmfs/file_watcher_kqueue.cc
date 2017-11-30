/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "backoff.h"
#include "file_watcher_kqueue.h"
#include "logging.h"
#include "util/posix.h"

namespace file_watcher {

FileWatcherKqueue::FileWatcherKqueue() {}

FileWatcherKqueue::~FileWatcherKqueue() {}

bool FileWatcherKqueue::RunEventLoop(const FileWatcher::HandlerMap& handlers,
                                     int control_pipe) {
  kq_ = kqueue();
  if (kq_ == -1) {
    LogCvmfs(kLogCvmfs, kLogDebug, "FileWatcherKqueue - Cannot create kqueue.");
    return false;
  }

  // Control pipe sending the stop event.
  struct kevent watch_event;
  EV_SET(&watch_event, control_pipe, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR,
         0, 0, 0);
  if (kevent(kq_, &watch_event, 1, NULL, 0, NULL) == -1) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "FileWatcherKqueue - Could not register event with kqueue.");
    return false;
  }

  for (FileWatcher::HandlerMap::const_iterator it = handlers.begin();
       it != handlers.end(); ++it) {
    RegisterFilter(it->first, it->second);
  }

  bool stop = false;
  while (!stop) {
    std::vector<struct kevent> triggered_events(handlers.size() + 1);
    int nev = kevent(kq_, NULL, 0, &triggered_events[0],
                     triggered_events.size(), NULL);
    if (nev == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "FileWatcherKqueue - Could not poll events.");
      return false;
    }
    if (nev == 0) {
      continue;
    }
    for (int i = 0; i < nev; ++i) {
      struct kevent& current_ev = triggered_events[i];
      if (current_ev.ident == static_cast<uint64_t>(control_pipe)) {
        char buffer[1];
        read(control_pipe, &buffer, 1);
        LogCvmfs(kLogCvmfs, kLogDebug, "FileWatcherKqueue - Stopping.\n");
        stop = true;
        continue;
      } else {
        std::map<int, WatchRecord>::const_iterator it =
            watch_records_.find(current_ev.ident);
        if (it != watch_records_.end()) {
          int current_fd = current_ev.ident;
          WatchRecord current_record = it->second;
          file_watcher::Event event = file_watcher::kInvalid;
          if (current_ev.fflags & NOTE_DELETE) {
            event = file_watcher::kDeleted;
          } else if (current_ev.fflags & NOTE_LINK) {
            // Hardlinked
            event = file_watcher::kHardlinked;
          } else if (current_ev.fflags & NOTE_WRITE) {
            // Modified
            event = file_watcher::kModified;
          } else if (current_ev.fflags & NOTE_RENAME) {
            // Renamed
            event = file_watcher::kRenamed;
          } else if (current_ev.fflags & NOTE_ATTRIB) {
            // Attributes
            event = file_watcher::kAttributes;
          }
          bool clear_handler = true;
          if (event != file_watcher::kInvalid) {
            current_record.handler_->Handle(current_record.file_path_, event,
                                            &clear_handler);
          } else {
            LogCvmfs(kLogCvmfs, kLogDebug,
                     "FileWatcherKqueue - Unknown event 0x%x\n",
                     current_ev.fflags);
          }

          // Perform post-handling actions (i.e. remove, reset filter)
          if (event == file_watcher::kDeleted) {
            const std::string file_path = watch_records_[current_fd].file_path_;
            EventHandler* handler = watch_records_[current_fd].handler_;
            RemoveFilter(current_fd);
            if (!clear_handler) {
              RegisterFilter(file_path, handler);
            }
          }
        } else {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "FileWatcherKqueue - Unknown kevent ident: %ld",
                   current_ev.ident);
        }
      }
    }
  }
  return true;
}

void FileWatcherKqueue::RemoveFilter(int fd) {
  struct kevent remove_event;
  EV_SET(&remove_event, fd, EVFILT_VNODE, EV_DELETE, NULL, 0, 0);
  if (kevent(kq_, &remove_event, 1, NULL, 0, NULL) == -1) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "FileWatcherKqueue - Could not remove event filter from kqueue.");
  }
  close(fd);
  watch_records_.erase(fd);
}

void FileWatcherKqueue::RegisterFilter(const std::string& file_path,
                                       EventHandler* handler) {
  bool done = false;
  BackoffThrottle throttle(1000, 10000, 50000);
  while (!done) {
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "FileWatcherKqueue - Cannot open file %s. Retrying.",
               file_path.c_str());
      throttle.Throttle();
      continue;
    }
    struct kevent watch_event;
    EV_SET(&watch_event, fd, EVFILT_VNODE, EV_ADD | EV_ENABLE | EV_CLEAR,
           NOTE_DELETE | NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB | NOTE_LINK |
               NOTE_RENAME | NOTE_REVOKE,
           0, 0);

    if (kevent(kq_, &watch_event, 1, NULL, 0, NULL) == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "FileWatcherKqueue - Could not register event with kqueue.");
      return;
    }

    watch_records_[fd] = WatchRecord(file_path, handler);

    done = true;
  }
  throttle.Reset();
}

}  // namespace file_watcher
