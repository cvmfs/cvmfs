/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "glue_buffer.h"
#include "platform.h"
#include "shortstring.h"
#include "util/posix.h"

namespace glue {

class T_GlueBuffer : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

 protected:
  InodeTracker inode_tracker_;
};


TEST_F(T_GlueBuffer, InodeTracker) {
  uint64_t inode_parent = 0;
  NameString name;
  uint64_t inode = 0;
  InodeTracker::Cursor cursor = inode_tracker_.BeginEnumerate();
  EXPECT_FALSE(inode_tracker_.NextEntry(&cursor, &inode_parent, &name));
  EXPECT_FALSE(inode_tracker_.NextEntry(&cursor, &inode_parent, &name));
  EXPECT_FALSE(inode_tracker_.NextInode(&cursor, &inode));
  inode_tracker_.EndEnumerate(&cursor);

  inode_tracker_.VfsGet(1, PathString(""));
  inode_tracker_.VfsGet(2, PathString("/foo"));
  inode_tracker_.VfsGet(4, PathString("/foo/bar"));
  cursor = inode_tracker_.BeginEnumerate();
  int bitset_entry = 0;
  int bitset_inode = 0;
  for (unsigned i = 0; i < 3; ++i) {
    EXPECT_TRUE(inode_tracker_.NextEntry(&cursor, &inode_parent, &name));
    EXPECT_TRUE(inode_tracker_.NextInode(&cursor, &inode));
    switch (inode_parent) {
      case 0:
        EXPECT_EQ("", name.ToString());
        bitset_entry |= 1;
        break;
      case 1:
        EXPECT_EQ("foo", name.ToString());
        bitset_entry |= 2;
        break;
      case 2:
        EXPECT_EQ("bar", name.ToString());
        bitset_entry |= 4;
        break;
      default:
        EXPECT_FALSE(true);
    }
    switch (inode) {
      case 1:
        bitset_inode |= 1;
        break;
      case 2:
        bitset_inode |= 2;
        break;
      case 4:
        bitset_inode |= 4;
        break;
      default:
        EXPECT_FALSE(true);
    }
  }
  EXPECT_EQ(7, bitset_entry);
  EXPECT_EQ(7, bitset_inode);
  EXPECT_FALSE(inode_tracker_.NextEntry(&cursor, &inode_parent, &name));
  EXPECT_FALSE(inode_tracker_.NextInode(&cursor, &inode));
  inode_tracker_.EndEnumerate(&cursor);
}


TEST_F(T_GlueBuffer, NentryTracker) {
  NentryTracker tracker;
  const unsigned kTimeoutNever = 100000;

  uint64_t parent_inode = 0;
  NameString name;
  NentryTracker::Cursor cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  tracker.Add(1, "one", kTimeoutNever);
  tracker.Add(2, "two", kTimeoutNever);
  tracker.Add(3, "ignore_me", 0);
  cursor = tracker.BeginEnumerate();
  EXPECT_TRUE(tracker.NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(1U, parent_inode);
  EXPECT_EQ(std::string("one"), name.ToString());
  EXPECT_TRUE(tracker.NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(2U, parent_inode);
  EXPECT_EQ(std::string("two"), name.ToString());
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  tracker.Disable();

  tracker.DoPrune(platform_monotonic_time() + kTimeoutNever + 1);
  cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  tracker.Add(4, "ignore_me", kTimeoutNever);
  cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  EXPECT_EQ(2U, tracker.GetStatistics().num_insert);
  EXPECT_EQ(2U, tracker.GetStatistics().num_remove);
  EXPECT_EQ(3U, tracker.GetStatistics().num_prune);
}


TEST_F(T_GlueBuffer, NentryMove) {
  NentryTracker tracker;
  const unsigned kTimeoutNever = 100000;

  tracker.Add(1, "one", kTimeoutNever);

  NentryTracker *dst = tracker.Move();

  uint64_t parent_inode = 0;
  NameString name;
  NentryTracker::Cursor cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  cursor = dst->BeginEnumerate();
  EXPECT_TRUE(dst->NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(1U, parent_inode);
  EXPECT_EQ(std::string("one"), name.ToString());
  EXPECT_FALSE(dst->NextEntry(&cursor, &parent_inode, &name));
  dst->EndEnumerate(&cursor);
}


TEST_F(T_GlueBuffer, NentryCleanerSlow) {
  NentryTracker tracker;
  tracker.Prune();  // Don't crash when tracker is empty

  tracker.Add(0, "one", 1);
  tracker.SpawnCleaner(1);
  SafeSleepMs(4000);

  EXPECT_GT(tracker.GetStatistics().num_prune, 0U);
  EXPECT_EQ(1U, tracker.GetStatistics().num_remove);
  EXPECT_EQ(1U, tracker.GetStatistics().num_insert);

  uint64_t parent_inode = 0;
  NameString name;
  NentryTracker::Cursor cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);
}

TEST_F(T_GlueBuffer, StringHeap) {
  {
    StringHeap string_heap;
    EXPECT_EQ(unsigned(128 * 1024), string_heap.GetSizeAlloc());
  }

  {
    StringHeap string_heap(1);
    EXPECT_EQ(unsigned(128 * 1024), string_heap.GetSizeAlloc());
  }

  {
    StringHeap string_heap(128 * 1024);
    EXPECT_EQ(unsigned(128 * 1024), string_heap.GetSizeAlloc());
  }

  {
    StringHeap string_heap(128 * 1024 + 1);
    EXPECT_EQ(unsigned(256 * 1024), string_heap.GetSizeAlloc());
  }

  if (sizeof(size_t) > 4) {
    uint64_t large_size;
    large_size = 1U << 31;
    large_size += 1;
    {
      StringHeap string_heap(large_size);
      EXPECT_EQ((uint64_t)(1) << 32, string_heap.GetSizeAlloc());
    }
  } else {
    printf("Skipping 64bit allocation test\n");
  }
}

}  // namespace glue
