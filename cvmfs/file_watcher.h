/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_WATCHER_H_
#define CVMFS_FILE_WATCHER_H_

#include <pthread.h>

#include <map>
#include <string>

enum FileWatcherEvent {
  kCreated,
  kDeleted,
  kModified
};

class FileWatcherEventHandler {
public:
  FileWatcherEventHandler();
  virtual ~FileWatcherEventHandler();

  virtual bool Handle(const std::string& file_path,
                      FileWatcherEvent event) = 0;
};

class FileWatcher {
public:
  FileWatcher();
  virtual ~FileWatcher();

  void RegisterHandler(const std::string& file_path,
                       FileWatcherEventHandler* handler);

  bool Start();

protected:
  typedef std::map<std::string, FileWatcherEventHandler*> HandlerMap;
  HandlerMap handler_map_;
  int control_pipe_[2];

  virtual void InitEventLoop() = 0;

private:
  static void* BackgroundThread(void* d);

  pthread_t thread_;
};

#endif  // CVMFS_FILE_WATCHER_H_
