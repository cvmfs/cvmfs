/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_WATCHER_INOTIFY_H_
#define CVMFS_FILE_WATCHER_INOTIFY_H_

#include <map>
#include <string>

#include "file_watcher.h"

namespace file_watcher {

class FileWatcherInotify : public FileWatcher {
 public:
  FileWatcherInotify();
  virtual ~FileWatcherInotify();

 protected:
  virtual bool RunEventLoop(const FileWatcher::HandlerMap &handler,
                            int read_pipe, int write_pipe);

  virtual int TryRegisterFilter(const std::string &file_path);

 private:
  int inotify_fd_;
};

}  // namespace file_watcher

#endif  // CVMFS_FILE_WATCHER_INOTIFY_H_
