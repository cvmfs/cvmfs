/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <map>

#include "logging.h"
#include "platform.h"
#include "util/pointer.h"
#include "util/posix.h"

typedef std::map<file_watcher::Event, int> Counters;

class TestEventHandler : public file_watcher::EventHandler {
 public:
  explicit TestEventHandler(Counters* ctrs) : counters_(ctrs) {}
  virtual ~TestEventHandler() {}

  virtual bool Handle(const std::string& /*file_path*/,
                      file_watcher::Event event,
                      bool* clear_handler) {
    (*counters_)[event]++;
    *clear_handler = true;
    return true;
  }

  Counters* counters_;
};

class T_FileWatcher : public ::testing::Test {
 protected:
  void SetUp() { counters_.clear(); }

  Counters counters_;
};

TEST_F(T_FileWatcher, ModifyThenDelete) {
  const std::string watched_file_name =
      GetCurrentWorkingDirectory() + "/file_watcher_test.txt";
  SafeWriteToFile("test", watched_file_name, 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(platform_file_watcher());

  TestEventHandler* hd = new TestEventHandler(&counters_);
  watcher->RegisterHandler(watched_file_name, hd);

  EXPECT_TRUE(watcher->Start());

  SafeWriteToFile("test", watched_file_name, 0600);

  SafeSleepMs(100);

  Counters::const_iterator it_mod = counters_.find(file_watcher::kModified);
  const int num_modifications = it_mod->second;
  EXPECT_EQ(1, num_modifications);

  unlink(watched_file_name.c_str());

  SafeSleepMs(100);

  Counters::const_iterator it_del = counters_.find(file_watcher::kDeleted);
  const int num_deletions = it_del->second;
  EXPECT_EQ(1, num_deletions);
}
