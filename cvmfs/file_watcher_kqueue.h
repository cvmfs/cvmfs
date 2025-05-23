/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_WATCHER_KQUEUE_H_
#define CVMFS_FILE_WATCHER_KQUEUE_H_

#include <map>
#include <string>

#include "file_watcher.h"

namespace file_watcher {

class FileWatcherKqueue : public FileWatcher {
 public:
  FileWatcherKqueue();
  virtual ~FileWatcherKqueue();

 protected:
  virtual bool RunEventLoop(const FileWatcher::HandlerMap &handler,
                            int read_pipe, int write_pipe);

  virtual int TryRegisterFilter(const std::string &file_path);

 private:
  void RemoveFilter(int fd);

  int kq_;
};

}  // namespace file_watcher

#endif  // CVMFS_FILE_WATCHER_KQUEUE_H_
