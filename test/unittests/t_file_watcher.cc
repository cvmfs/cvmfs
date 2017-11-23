/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "file_watcher.h"
#include "logging.h"
#include "util/pointer.h"
#include "util/posix.h"

class TestEventHandler : public FileWatcherEventHandler {
public:
  TestEventHandler() {}
  virtual ~TestEventHandler() {}

  virtual bool Handle(const std::string& file_path,
                      FileWatcherEvent event) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Handling %d event for file: %s",
             event, file_path.c_str());
    return true;
  }
};

class TestFileWatcher : public FileWatcher {
public:
  TestFileWatcher() : FileWatcher() {}
  virtual ~TestFileWatcher() {}

  virtual void InitEventLoop() {
  }
};

class T_FileWatcher : public ::testing::Test {};

TEST_F(T_FileWatcher, Dummy) {
  UniquePtr<FileWatcher> watcher(new TestFileWatcher());

  TestEventHandler* hd = new TestEventHandler;
  watcher->RegisterHandler("/tmp/file_watcher_test.txt", hd);

  EXPECT_TRUE(watcher->Start());
  SafeSleepMs(10);
}
