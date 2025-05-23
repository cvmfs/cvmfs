/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <map>

#include "file_watcher.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"

typedef std::map<file_watcher::Event, int> Counters;

class TestEventHandler : public file_watcher::EventHandler {
 public:
  typedef std::set<file_watcher::Event> EventMask;

  explicit TestEventHandler(Counters *ctrs, FifoChannel<bool> *chan)
      : mask_(), counters_(ctrs), chan_(chan), clear_(true) { }

  virtual ~TestEventHandler() { }

  // The event mask contains the events which are of interest for the test
  // By default, all events are considered to be of interest
  void SetEventMask(const EventMask &mask) { mask_ = mask; }

  virtual bool Handle(const std::string & /*file_path*/,
                      file_watcher::Event event,
                      bool *clear_handler) {
    if (mask_.empty() || (mask_.count(event) == 1)) {
      (*counters_)[event]++;
      *clear_handler = clear_;
      chan_->Enqueue(true);
    }
    return true;
  }

  EventMask mask_;
  Counters *counters_;
  FifoChannel<bool> *chan_;
  bool clear_;
};

class T_FileWatcher : public ::testing::Test {
 protected:
  void SetUp() {
    counters_.clear();
    channel_ = new FifoChannel<bool>(10, 1);
  }

  Counters counters_;
  UniquePtr<FifoChannel<bool> > channel_;
};

TEST_F(T_FileWatcher, NoEventStop) {
  const std::string watched_file_name = GetCurrentWorkingDirectory()
                                        + "/file_watcher_test.txt";
  SafeWriteToFile("test", watched_file_name, 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(
      file_watcher::FileWatcher::Create());
  EXPECT_TRUE(watcher.IsValid());

  TestEventHandler *hd(new TestEventHandler(&counters_, channel_.weak_ref()));
  watcher->RegisterHandler(watched_file_name, hd);

  EXPECT_TRUE(watcher->Spawn());

  watcher->Stop();
}

TEST_F(T_FileWatcher, ModifiedEvent) {
  const std::string watched_file_name = GetCurrentWorkingDirectory()
                                        + "/file_watcher_test.txt";
  SafeWriteToFile("test", watched_file_name, 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(
      file_watcher::FileWatcher::Create());
  EXPECT_TRUE(watcher.IsValid());

  TestEventHandler *hd(new TestEventHandler(&counters_, channel_.weak_ref()));
  TestEventHandler::EventMask mask;
  mask.insert(file_watcher::kModified);
  hd->SetEventMask(mask);
  watcher->RegisterHandler(watched_file_name, hd);

  EXPECT_TRUE(watcher->Spawn());

  SafeWriteToFile("test", watched_file_name, 0600);

  channel_->Dequeue();

  Counters::const_iterator it_mod = counters_.find(file_watcher::kModified);
  const int num_modifications = it_mod->second;
  EXPECT_EQ(1, num_modifications);

  watcher->Stop();
}

TEST_F(T_FileWatcher, DeletedEvent) {
  const std::string watched_file_name = GetCurrentWorkingDirectory()
                                        + "/file_watcher_test2.txt";
  SafeWriteToFile("test", watched_file_name, 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(
      file_watcher::FileWatcher::Create());
  EXPECT_TRUE(watcher.IsValid());

  TestEventHandler *hd(new TestEventHandler(&counters_, channel_.weak_ref()));
  TestEventHandler::EventMask mask;
  mask.insert(file_watcher::kDeleted);
  hd->SetEventMask(mask);
  watcher->RegisterHandler(watched_file_name, hd);

  EXPECT_TRUE(watcher->Spawn());

  unlink(watched_file_name.c_str());

  channel_->Dequeue();

  Counters::const_iterator it_del = counters_.find(file_watcher::kDeleted);
  const int num_deletions = it_del->second;
  EXPECT_EQ(1, num_deletions);

  watcher->Stop();
}

TEST_F(T_FileWatcher, ModifiedThenDeletedEvent) {
  const std::string watched_file_name = GetCurrentWorkingDirectory()
                                        + "/file_watcher_test.txt";
  SafeWriteToFile("test", watched_file_name, 0600);

  UniquePtr<file_watcher::FileWatcher> watcher(
      file_watcher::FileWatcher::Create());
  EXPECT_TRUE(watcher.IsValid());

  TestEventHandler *hd(new TestEventHandler(&counters_, channel_.weak_ref()));
  TestEventHandler::EventMask mask;
  mask.insert(file_watcher::kModified);
  mask.insert(file_watcher::kDeleted);
  hd->SetEventMask(mask);
  hd->clear_ = false;
  watcher->RegisterHandler(watched_file_name, hd);

  EXPECT_TRUE(watcher->Spawn());

  SafeWriteToFile("test", watched_file_name, 0600);

  channel_->Dequeue();

  hd->clear_ = true;

  Counters::const_iterator it_mod = counters_.find(file_watcher::kModified);
  const int num_modifications = it_mod->second;
  EXPECT_EQ(1, num_modifications);

  unlink(watched_file_name.c_str());

  channel_->Dequeue();

  Counters::const_iterator it_del = counters_.find(file_watcher::kDeleted);
  const int num_deletions = it_del->second;
  EXPECT_EQ(1, num_deletions);

  watcher->Stop();
}
