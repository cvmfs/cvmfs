/**
 * This file is part of the CernVM File System.
 */

#include "file_watcher.h"

#include <unistd.h>

#include <cassert>

#include "backoff.h"

#ifdef __APPLE__
#include "file_watcher_kqueue.h"
#else
#ifdef CVMFS_ENABLE_INOTIFY
#include "file_watcher_inotify.h"
#endif
#endif

#include "util/logging.h"
#include "util/posix.h"

namespace file_watcher {

EventHandler::EventHandler() { }

EventHandler::~EventHandler() { }

const unsigned FileWatcher::kInitialDelay = 1000;
const unsigned FileWatcher::kMaxDelay = 10000;
const unsigned FileWatcher::kResetDelay = 50000;

FileWatcher::FileWatcher()
    : handler_map_()
    , control_pipe_to_back_()
    , control_pipe_to_front_()
    , started_(false) { }

FileWatcher::~FileWatcher() { Stop(); }

FileWatcher *FileWatcher::Create() {
#ifdef __APPLE__
  return new file_watcher::FileWatcherKqueue();
#else
#ifdef CVMFS_ENABLE_INOTIFY
  return new file_watcher::FileWatcherInotify();
#else
  return NULL;
#endif
#endif
}

void FileWatcher::RegisterHandler(const std::string &file_path,
                                  EventHandler *handler) {
  handler_map_[file_path] = handler;
}

bool FileWatcher::Spawn() {
  if (started_) {
    return false;
  }

  MakePipe(control_pipe_to_back_);
  MakePipe(control_pipe_to_front_);

  assert(pthread_create(&thread_, NULL, &FileWatcher::BackgroundThread, this)
         == 0);

  // Before returning, wait for a start signal in the control pipe
  // from the background thread.
  char buffer[1];
  ReadHalfPipe(control_pipe_to_front_[0], buffer, 1);

  started_ = true;
  return true;
}

void FileWatcher::Stop() {
  if (!started_) {
    return;
  }

  WritePipe(control_pipe_to_back_[1], "q", 1);
  assert(pthread_join(thread_, NULL) == 0);

  ClosePipe(control_pipe_to_front_);
  ClosePipe(control_pipe_to_back_);

  for (HandlerMap::iterator it = handler_map_.begin(); it != handler_map_.end();
       ++it) {
    delete it->second;
  }

  started_ = false;
}

void *FileWatcher::BackgroundThread(void *d) {
  FileWatcher *watcher = reinterpret_cast<FileWatcher *>(d);

  if (!watcher->RunEventLoop(watcher->handler_map_,
                             watcher->control_pipe_to_back_[0],
                             watcher->control_pipe_to_front_[1])) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Error running event loop.");
  }

  pthread_exit(NULL);
}

void FileWatcher::RegisterFilter(const std::string &file_path,
                                 EventHandler *handler) {
  bool done = false;
  BackoffThrottle throttle(kInitialDelay, kMaxDelay, kResetDelay);
  while (!done) {
    int wd = TryRegisterFilter(file_path);
    if (wd < 0) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "FileWatcher - Could not add watch for file %s. Retrying.",
               file_path.c_str());
      throttle.Throttle();
      continue;
    }

    watch_records_[wd] = WatchRecord(file_path, handler);

    done = true;
  }
  throttle.Reset();
}

}  // namespace file_watcher
