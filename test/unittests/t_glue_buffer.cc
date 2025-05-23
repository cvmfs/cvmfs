/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "glue_buffer.h"
#include "shortstring.h"
#include "smallhash.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/prng.h"

namespace glue {

class T_GlueBuffer : public ::testing::Test {
 protected:
  virtual void SetUp() { }

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

  inode_tracker_.VfsGet(glue::InodeEx(1, glue::InodeEx::kDirectory),
                        PathString(""));
  inode_tracker_.VfsGet(glue::InodeEx(2, glue::InodeEx::kDirectory),
                        PathString("/foo"));
  inode_tracker_.VfsGet(glue::InodeEx(4, glue::InodeEx::kRegular),
                        PathString("/foo/bar"));
  inode_tracker_.VfsGet(glue::InodeEx(8, glue::InodeEx::kSymlink),
                        PathString("/foo/bar/baz"));
  cursor = inode_tracker_.BeginEnumerate();
  int bitset_entry = 0;
  int bitset_inode = 0;
  for (unsigned i = 0; i < 4; ++i) {
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
      case 4:
        EXPECT_EQ("baz", name.ToString());
        bitset_entry |= 8;
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
      case 8:
        bitset_inode |= 8;
        break;
      default:
        EXPECT_FALSE(true);
    }
  }
  EXPECT_EQ(15, bitset_entry);
  EXPECT_EQ(15, bitset_inode);
  EXPECT_FALSE(inode_tracker_.NextEntry(&cursor, &inode_parent, &name));
  EXPECT_FALSE(inode_tracker_.NextInode(&cursor, &inode));
  inode_tracker_.EndEnumerate(&cursor);

  EXPECT_FALSE(inode_tracker_.FindDentry(42, &inode_parent, &name));
  EXPECT_TRUE(inode_tracker_.FindDentry(4, &inode_parent, &name));
  EXPECT_STREQ("bar", name.c_str());
  EXPECT_EQ(2U, inode_parent);
}


TEST_F(T_GlueBuffer, DentryTracker) {
  DentryTracker tracker;
  const unsigned kTimeoutNever = 100000;

  uint64_t parent_inode = 0;
  NameString name;
  DentryTracker::Cursor cursor = tracker.BeginEnumerate();
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


TEST_F(T_GlueBuffer, DentryMove) {
  DentryTracker tracker;
  const unsigned kTimeoutNever = 100000;

  tracker.Add(1, "one", kTimeoutNever);

  DentryTracker *dst = tracker.Move();

  uint64_t parent_inode = 0;
  NameString name;
  DentryTracker::Cursor cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  cursor = dst->BeginEnumerate();
  EXPECT_TRUE(dst->NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(1U, parent_inode);
  EXPECT_EQ(std::string("one"), name.ToString());
  EXPECT_FALSE(dst->NextEntry(&cursor, &parent_inode, &name));
  dst->EndEnumerate(&cursor);
}


TEST_F(T_GlueBuffer, DentryCleanerSlow) {
  DentryTracker tracker;
  tracker.Prune();  // Don't crash when tracker is empty

  tracker.Add(0, "one", 1);
  tracker.SpawnCleaner(1);
  SafeSleepMs(4000);

  EXPECT_GT(tracker.GetStatistics().num_prune, 0U);
  EXPECT_EQ(1U, tracker.GetStatistics().num_remove);
  EXPECT_EQ(1U, tracker.GetStatistics().num_insert);

  uint64_t parent_inode = 0;
  NameString name;
  DentryTracker::Cursor cursor = tracker.BeginEnumerate();
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

TEST_F(T_GlueBuffer, StatStore) {
  StatStore store;
  struct stat info;
  info.st_ino = 42;
  EXPECT_EQ(0, store.Add(info));
  EXPECT_EQ(42u, store.Erase(0));

  std::vector<int32_t> indexes;
  for (int i = 0; i < 1000; ++i) {
    info.st_ino = i;
    indexes.push_back(store.Add(info));
    EXPECT_EQ(i, indexes.back());
  }

  Prng prng;
  prng.InitLocaltime();
  std::set<uint64_t> inodes;
  for (int i = 0; i < 1000; ++i) {
    int32_t index = static_cast<int32_t>(prng.Next(1000 - i));
    info = store.Get(index);
    inodes.insert(info.st_ino);

    uint64_t inode = store.Erase(index);
    EXPECT_LE(0U, inode);
    EXPECT_LT(inode, 1000U);

    if (index < (1000 - (i + 1))) {
      info = store.Get(index);
      EXPECT_EQ(inode, info.st_ino);
    }
  }
  EXPECT_EQ(1000U, inodes.size());
}

TEST_F(T_GlueBuffer, PageCacheTrackerOff) {
  PageCacheTracker tracker;
  tracker.Disable();
  struct stat info;
  info.st_ino = 1;
  PageCacheTracker::OpenDirectives directives;
  directives = tracker.Open(1, shash::Any(), info);
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);
  // Don't crash on unknown inode
  tracker.Close(2);
  tracker.GetEvictRaii().Evict(3);
}

TEST_F(T_GlueBuffer, PageCacheTrackerBasics) {
  PageCacheTracker tracker;
  PageCacheTracker::OpenDirectives directives;
  struct stat info;
  info.st_ino = 1;

  shash::Any hashA(shash::kShake128);
  shash::Any hashB(shash::kShake128);
  shash::HashString("A", &hashA);
  shash::HashString("B", &hashB);

  directives = tracker.Open(1, hashA, info);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashA, info);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashB, info);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(true, directives.direct_io);

  EXPECT_DEATH(tracker.Close(2), ".*");
  tracker.Close(1);
  tracker.Close(1);
  EXPECT_DEATH(tracker.Close(1), ".*");

  directives = tracker.Open(1, hashB, info);
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  directives = tracker.Open(1, hashB, info);
  EXPECT_EQ(false, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  tracker.Close(1);

  directives = tracker.Open(1, hashB, info);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);

  tracker.Close(1);
  tracker.Close(1);

  tracker.GetEvictRaii().Evict(1);
  directives = tracker.Open(1, hashA, info);
  EXPECT_EQ(true, directives.keep_cache);
  EXPECT_EQ(false, directives.direct_io);
}

TEST_F(T_GlueBuffer, PageCacheTrackerStat) {
  PageCacheTracker tracker;
  struct stat info;
  info.st_ino = 42;

  shash::Any hash(shash::kShake128);
  shash::HashString("X", &hash);
  struct stat ress;
  shash::Any resh;

  tracker.Open(42, hash, info);
  EXPECT_TRUE(tracker.GetInfoIfOpen(42, &resh, &ress));
  EXPECT_EQ(42U, ress.st_ino);
  EXPECT_EQ(hash, resh);

  tracker.Open(42, hash, info);
  EXPECT_EQ(42U, ress.st_ino);
  EXPECT_EQ(hash, resh);

  tracker.Close(42);
  tracker.Close(42);
  EXPECT_FALSE(tracker.GetInfoIfOpen(42, &resh, &ress));
}

TEST_F(T_GlueBuffer, InodeEx) {
  InodeEx inode_ex(0, InodeEx::kUnknownType);
  EXPECT_EQ(sizeof(uint64_t), sizeof(inode_ex));

  EXPECT_EQ(0U, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kUnknownType, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(0), hasher_inode_ex(inode_ex));

  inode_ex = InodeEx(1, InodeEx::kRegular);
  EXPECT_EQ(1U, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kRegular, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(1), hasher_inode_ex(inode_ex));

  uint64_t largest_inode = (uint64_t(1) << 60) - 1;
  inode_ex = InodeEx(largest_inode, InodeEx::kBulkDev);
  EXPECT_EQ(largest_inode, inode_ex.GetInode());
  EXPECT_EQ(InodeEx::kBulkDev, inode_ex.GetFileType());
  EXPECT_EQ(hasher_inode(largest_inode), hasher_inode_ex(inode_ex));

  SmallHashDynamic<InodeEx, char> map;
  map.Init(16, InodeEx(), hasher_inode_ex);
  map.Insert(InodeEx(42, InodeEx::kRegular), 1);
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kRegular)));
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kSymlink)));
  EXPECT_FALSE(map.Contains(InodeEx(43, InodeEx::kRegular)));
  InodeEx key(43, InodeEx::kUnknownType);
  char value = 0;
  EXPECT_FALSE(map.LookupEx(&key, &value));
  key = InodeEx(42, InodeEx::kUnknownType);
  EXPECT_TRUE(map.LookupEx(&key, &value));
  EXPECT_EQ(1, value);
  EXPECT_EQ(42U, key.GetInode());
  EXPECT_EQ(InodeEx::kRegular, key.GetFileType());

  map.Insert(InodeEx(42, InodeEx::kSymlink), 2);
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kRegular)));
  EXPECT_TRUE(map.Contains(InodeEx(42, InodeEx::kSymlink)));
  EXPECT_FALSE(map.Contains(InodeEx(43, InodeEx::kRegular)));
  EXPECT_TRUE(map.LookupEx(&key, &value));
  EXPECT_EQ(2, value);
  EXPECT_EQ(42U, key.GetInode());
  EXPECT_EQ(InodeEx::kSymlink, key.GetFileType());

  // Check that the original key survives map resizings
  for (unsigned i = 100; i < 1000; ++i) {
    map.Insert(InodeEx(i, InodeEx::kRegular), 0);
  }
  key = InodeEx(42, InodeEx::kUnknownType);
  EXPECT_TRUE(map.LookupEx(&key, &value));
  EXPECT_EQ(2, value);
  EXPECT_EQ(42U, key.GetInode());
  EXPECT_EQ(InodeEx::kSymlink, key.GetFileType());
}

TEST_F(T_GlueBuffer, InodeExMode) {
  platform_stat64 info;
  EXPECT_EQ(0, platform_stat(".", &info));
  InodeEx inode_ex(42, info.st_mode);
  EXPECT_EQ(InodeEx::kDirectory, inode_ex.GetFileType());
  EXPECT_EQ(42U, inode_ex.GetInode());

  EXPECT_EQ(0, platform_stat("/", &info));
  EXPECT_TRUE(inode_ex.IsCompatibleFileType(info.st_mode));
  CreateFile("regular", 0644);

  EXPECT_EQ(0, platform_stat("regular", &info));
  EXPECT_FALSE(inode_ex.IsCompatibleFileType(info.st_mode));

  inode_ex = InodeEx(42, InodeEx::kUnknownType);
  EXPECT_TRUE(inode_ex.IsCompatibleFileType(info.st_mode));
}

}  // namespace glue
