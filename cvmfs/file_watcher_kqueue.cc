/**
 * This file is part of the CernVM File System.
 */

#include "file_watcher_kqueue.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <string>
#include <vector>

#include "backoff.h"
#include "util/logging.h"
#include "util/posix.h"

namespace file_watcher {

FileWatcherKqueue::FileWatcherKqueue() { }

FileWatcherKqueue::~FileWatcherKqueue() { }

bool FileWatcherKqueue::RunEventLoop(const FileWatcher::HandlerMap &handlers,
                                     int read_pipe, int write_pipe) {
  kq_ = kqueue();
  assert(kq_ != -1);

  // Control pipe sending the stop event.
  struct kevent watch_event;
  EV_SET(&watch_event, read_pipe, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0,
         0, 0);
  assert(kevent(kq_, &watch_event, 1, NULL, 0, NULL) != -1);

  for (FileWatcher::HandlerMap::const_iterator it = handlers.begin();
       it != handlers.end();
       ++it) {
    RegisterFilter(it->first, it->second);
  }

  // Use the control pipe to signal readiness to the main thread,
  // before continuing with the event loop.
  WritePipe(write_pipe, "s", 1);

  bool stop = false;
  while (!stop) {
    std::vector<struct kevent> triggered_events(handlers.size() + 1);
    int nev = kevent(kq_, NULL, 0, &triggered_events[0],
                     triggered_events.size(), NULL);
    if (nev == -1) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "FileWatcherKqueue - Could not poll events. Errno: %d", errno);
      return false;
    }
    if (nev == 0) {
      continue;
    }
    for (int i = 0; i < nev && !stop; ++i) {
      struct kevent &current_ev = triggered_events[i];
      if (current_ev.ident == static_cast<uint64_t>(read_pipe)) {
        char buffer[1];
        ReadPipe(read_pipe, &buffer, 1);
        LogCvmfs(kLogCvmfs, kLogDebug, "FileWatcherKqueue - Stopping.\n");
        stop = true;
        continue;
      }

      std::map<int, WatchRecord>::const_iterator it = watch_records_.find(
          current_ev.ident);
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
          RemoveFilter(current_fd);
          if (!clear_handler) {
            RegisterFilter(current_record.file_path_, current_record.handler_);
          }
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "FileWatcherKqueue - Unknown kevent ident: %ld",
                 current_ev.ident);
      }
    }
  }

  // Close all remaining open file descriptors
  for (std::map<int, WatchRecord>::const_iterator it = watch_records_.begin();
       it != watch_records_.end();
       ++it) {
    close(it->first);
  }
  watch_records_.clear();

  close(kq_);

  return true;
}

void FileWatcherKqueue::RemoveFilter(int fd) {
  struct kevent remove_event;
  EV_SET(&remove_event, fd, EVFILT_VNODE, EV_DELETE, NULL, 0, 0);
  assert(kevent(kq_, &remove_event, 1, NULL, 0, NULL) != -1);
  watch_records_.erase(fd);
  close(fd);
}

int FileWatcherKqueue::TryRegisterFilter(const std::string &file_path) {
  int fd = open(file_path.c_str(), O_RDONLY);
  if (fd > 0) {
    struct kevent watch_event;
    EV_SET(&watch_event, fd, EVFILT_VNODE, EV_ADD | EV_ENABLE | EV_CLEAR,
           NOTE_DELETE | NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB | NOTE_LINK
               | NOTE_RENAME | NOTE_REVOKE,
           0, 0);

    assert(kevent(kq_, &watch_event, 1, NULL, 0, NULL) != -1);
  }

  return fd;
}

}  // namespace file_watcher
