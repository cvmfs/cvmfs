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
  kIgnored,
  kInvalid
};

class EventHandler {
 public:
  EventHandler();
  virtual ~EventHandler();

  /**
   * Handle function called per event
   *
   * @param file_path - the path of the file this event corresponds to
   * @param event - the type of event
   * @param clear_handler - (output) set this to true to have the event
   *                        handler remove from the loop after this call
   *
   * Should return false in case of failure.
   *
   * Setting the clear_handler parameter to false means that the FileWatcher
   * object will attempt to re-register the handler for the same file name
   * in the case a file was delete - useful for continuously watching a file
   * which may be deleted and recreated
   */
  virtual bool Handle(const std::string &file_path,
                      Event event,
                      bool *clear_handler) = 0;
};

struct WatchRecord {
  WatchRecord() : file_path_(), handler_(NULL) { }

  WatchRecord(const std::string &path, file_watcher::EventHandler *h)
      : file_path_(path), handler_(h) { }

  std::string file_path_;
  file_watcher::EventHandler *handler_;
};

class FileWatcher {
 public:
  typedef std::map<std::string, EventHandler *> HandlerMap;

  FileWatcher();
  virtual ~FileWatcher();
  static FileWatcher *Create();

  void RegisterHandler(const std::string &file_path, EventHandler *handler);

  bool Spawn();

  void Stop();

 protected:
  // Delays controlling the backoff throttle when registering new watches
  static const unsigned kInitialDelay;
  static const unsigned kMaxDelay;
  static const unsigned kResetDelay;

  void RegisterFilter(const std::string &file_path, EventHandler *handler);

  virtual bool RunEventLoop(const HandlerMap &handler_map, int read_pipe,
                            int write_pipe) = 0;

  virtual int TryRegisterFilter(const std::string &file_path) = 0;

  std::map<int, WatchRecord> watch_records_;

 private:
  static void *BackgroundThread(void *d);

  HandlerMap handler_map_;

  int control_pipe_to_back_[2];
  int control_pipe_to_front_[2];

  pthread_t thread_;

  bool started_;
};

}  // namespace file_watcher

#endif  // CVMFS_FILE_WATCHER_H_
