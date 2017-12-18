/**
 * This file is part of the CernVM File System.
 */

#include "file_watcher.h"

#include <unistd.h>

#include <cassert>

#include "logging.h"
#include "util/posix.h"

namespace file_watcher {

EventHandler::EventHandler() {}

EventHandler::~EventHandler() {}

FileWatcher::FileWatcher()
    : handler_map_()
    , control_pipe_to_back_()
    , control_pipe_to_front_()
    , started_(false) {}

FileWatcher::~FileWatcher() {
    Stop();
}

void FileWatcher::RegisterHandler(const std::string& file_path,
                                  EventHandler* handler) {
  handler_map_[file_path] = handler;
}

bool FileWatcher::Spawn() {
  if (started_) {
    return false;
  }

  MakePipe(control_pipe_to_back_);
  MakePipe(control_pipe_to_front_);

  assert(pthread_create(&thread_, NULL,
                        &FileWatcher::BackgroundThread, this) == 0);

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

void* FileWatcher::BackgroundThread(void* d) {
  FileWatcher* watcher = reinterpret_cast<FileWatcher*>(d);

  if (!watcher->RunEventLoop(watcher->handler_map_,
                             watcher->control_pipe_to_back_[0],
                             watcher->control_pipe_to_front_[1])) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Error running event loop.");
  }

  pthread_exit(NULL);
}

}  // namespace file_watcher
