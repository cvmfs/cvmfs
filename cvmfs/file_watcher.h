/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_WATCHER_H_
#define CVMFS_FILE_WATCHER_H_

#include <pthread.h>

#include <map>
#include <string>

namespace file_watcher {

enum Event {
  kModified,
  kRenamed,
  kAttributes,
  kHardlinked,
  kDeleted,
  kInvalid
};

class EventHandler {
 public:
  EventHandler();
  virtual ~EventHandler();

  virtual bool Handle(const std::string& file_path,
                      Event event) = 0;
};

class FileWatcher {
 public:
  typedef std::map<std::string, EventHandler*> HandlerMap;

  FileWatcher();
  virtual ~FileWatcher();

  void RegisterHandler(const std::string& file_path,
                       EventHandler* handler);

  bool Start();

 protected:
  virtual bool RunEventLoop(const HandlerMap& handler_map,
                            int control_pipe) = 0;

 private:
  static void* BackgroundThread(void* d);

  HandlerMap handler_map_;
  int control_pipe_[2];
  pthread_t thread_;

  bool started_;
};

}  // namespace file_watcher

#endif  // CVMFS_FILE_WATCHER_H_
