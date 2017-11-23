/**
 * This file is part of the CernVM File System.
 */

#include "file_watcher.h"

#include <unistd.h>

#include "logging.h"


FileWatcherEventHandler::FileWatcherEventHandler() {}

FileWatcherEventHandler::~FileWatcherEventHandler() {}

FileWatcher::FileWatcher()
    : handler_map_(),
      control_pipe_() {}

FileWatcher::~FileWatcher() {
  write(control_pipe_[1], "q", 1);
  int ret = pthread_join(thread_, NULL);
  if (ret != 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Could not join monitor thread: %d\n", ret);
  }

  close(control_pipe_[1]);
  close(control_pipe_[0]);

  for (HandlerMap::iterator it = handler_map_.begin();
       it != handler_map_.end(); ++it) {
    delete it->second;
  }
}

void FileWatcher::RegisterHandler(const std::string& file_path,
                                  FileWatcherEventHandler* handler) {
  handler_map_[file_path] = handler;
}

bool FileWatcher::Start() {
  int ret = pipe(control_pipe_);
  if (ret != 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Could not create control pipe: %d\n", ret);
    return false;
  }

  ret = pthread_create(&thread_, NULL, &FileWatcher::BackgroundThread, this);
  if (ret != 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Could not create monitor thread: %d\n", ret);
    return false;
  }

  return true;
}

void* FileWatcher::BackgroundThread(void* d) {
  FileWatcher* watcher = reinterpret_cast<FileWatcher*>(d);

  watcher->InitEventLoop();

  pthread_exit(NULL);
}
