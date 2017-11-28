/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <map>

#include "file_watcher_kqueue.h"
#include "logging.h"
#include "util/pointer.h"
#include "util/posix.h"

typedef std::map<file_watcher::Event, int> Counters;

class TestEventHandler : public file_watcher::EventHandler {
public:
  TestEventHandler(Counters* ctrs) : counters_(ctrs) {}
  virtual ~TestEventHandler() {}

  virtual bool Handle(const std::string& file_path,
                      file_watcher::Event event) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Handling %d event for file: %s",
             event, file_path.c_str());
    ((*counters_)[event])++;
    return true;
  }

  Counters* counters_;
};

class T_FileWatcherKqueue : public ::testing::Test {
protected:
  void SetUp() {
    counters_.clear();
  }

  Counters counters_;
};

TEST_F(T_FileWatcherKqueue, Dummy) {
  SafeWriteToFile("test", "/tmp/file_watcher_test.txt", 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(new file_watcher::FileWatcherKqueue());

  TestEventHandler* hd = new TestEventHandler(&counters_);
  watcher->RegisterHandler("/tmp/file_watcher_test.txt", hd);

  EXPECT_TRUE(watcher->Start());

  SafeWriteToFile("test", "/tmp/file_watcher_test.txt", 0600);

  for (Counters::const_iterator it = counters_.begin(); it != counters_.end(); ++it) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Counters[%d] = %d", it->first, it->second);
  }
}

