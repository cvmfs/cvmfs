/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "glue_buffer.h"
#include "platform.h"
#include "shortstring.h"
#include "smallhash.h"
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
      pid_t child_pid = fork();
      switch (child_pid) {
      case 0: {
        // May crash due to memory constraints
        StringHeap string_heap(large_size);
        EXPECT_EQ((uint64_t)(1) << 32, string_heap.GetSizeAlloc());
        exit(0);
      }
      default:
        ASSERT_GT(child_pid, 0);
        int status;
        waitpid(child_pid, &status, 0);
        EXPECT_TRUE(WIFEXITED(status));
        EXPECT_EQ(0, WEXITSTATUS(status));
      }
    }
  } else {
    printf("Skipping 64bit allocation test\n");
  }
}

TEST_F(T_GlueBuffer, PageCacheTrackerOff) {
  PageCacheTracker tracker;
  tracker.Disable();
  PageCacheTracker::OpenDirectives directives;
  directives = tracker.Open(1, shash::Any());
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);
  // Don't crash on unknown inode
  tracker.Close(2);
  tracker.GetEvictRaii().Evict(3);
}

TEST_F(T_GlueBuffer, PageCacheTrackerBasics) {
  PageCacheTracker tracker;
  PageCacheTracker::OpenDirectives directives;

  shash::Any hashA(shash::kShake128);
  shash::Any hashB(shash::kShake128);
  shash::HashString("A", &hashA);
  shash::HashString("B", &hashB);

  directives = tracker.Open(1, hashA);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashA);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashB);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(true, directives.direct_io);

  EXPECT_DEATH(tracker.Close(2), ".*");
  tracker.Close(1);
  tracker.Close(1);
  EXPECT_DEATH(tracker.Close(1), ".*");

  directives = tracker.Open(1, hashB);
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashB);
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  tracker.Close(1);

  directives = tracker.Open(1, hashB);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  tracker.Close(1);
  tracker.Close(1);

  tracker.GetEvictRaii().Evict(1);
  directives = tracker.Open(1, hashA);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);
}

TEST_F(T_GlueBuffer, InodeEx) {
  InodeEx inode_ex(0, InodeEx::kUnset);
  EXPECT_EQ(sizeof(std::uint64_t), sizeof(inode_ex));

  EXPECT_EQ(0U, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kUnset, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(0), hasher_inode_ex(inode_ex));

  inode_ex = InodeEx(1, InodeEx::kRegular);
  EXPECT_EQ(1U, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kRegular, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(1), hasher_inode_ex(inode_ex));

  uint64_t largest_inode = (uint64_t(1) << 61) - 1;
  inode_ex = InodeEx(largest_inode, InodeEx::kBulkDev);
  EXPECT_EQ(largest_inode, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kBulkDev, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(largest_inode), hasher_inode_ex(inode_ex));

  SmallHashDynamic<InodeEx, char> map;
  map.Init(16, InodeEx(), hasher_inode_ex);
  map.Insert(InodeEx(42, InodeEx::kRegular), 0);
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kRegular)));
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kSymlink)));
  EXPECT_FALSE(map.Contains(InodeEx(43, InodeEx::kRegular)));

  map.Insert(InodeEx(42, InodeEx::kSymlink), 0);
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kRegular)));
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kSymlink)));
  EXPECT_FALSE(map.Contains(InodeEx(43, InodeEx::kRegular)));
}

}  // namespace glue
