/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "file_watcher.h"
#include "logging.h"
#include "util/pointer.h"

class TestEventHandler : public file_watcher::EventHandler {
 public:
  TestEventHandler() {}
  virtual ~TestEventHandler() {}

  virtual bool Handle(const std::string& file_path,
                      file_watcher::Event event,
                      bool* clear_handler) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Handling %d event for file: %s",
             event, file_path.c_str());
    *clear_handler = true;
    return true;
  }
};

class TestFileWatcher : public file_watcher::FileWatcher {
 public:
  TestFileWatcher() : FileWatcher() {}
  virtual ~TestFileWatcher() {}

  virtual bool RunEventLoop(const FileWatcher::HandlerMap&, int) {
    return true;
  }
};

class T_FileWatcher : public ::testing::Test {};

TEST_F(T_FileWatcher, Dummy) {
  UniquePtr<file_watcher::FileWatcher> watcher(new TestFileWatcher());

  TestEventHandler* hd = new TestEventHandler;
  watcher->RegisterHandler("/tmp/file_watcher_test.txt", hd);

  EXPECT_TRUE(watcher->Start());
}
