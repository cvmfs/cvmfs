/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "glue_buffer.h"
#include "platform.h"
#include "shortstring.h"

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
  NentryTracker tracker(100000);  // Don't auto-prune

  uint64_t parent_inode = 0;
  NameString name;
  NentryTracker::Cursor cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  tracker.Add(1, "one");
  tracker.Add(2, "two");
  cursor = tracker.BeginEnumerate();
  EXPECT_TRUE(tracker.NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(1U, parent_inode);
  EXPECT_EQ(std::string("one"), name.ToString());
  EXPECT_TRUE(tracker.NextEntry(&cursor, &parent_inode, &name));
  EXPECT_EQ(2U, parent_inode);
  EXPECT_EQ(std::string("two"), name.ToString());
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  tracker.DoPrune(platform_monotonic_time() + 100000 + 1);
  cursor = tracker.BeginEnumerate();
  EXPECT_FALSE(tracker.NextEntry(&cursor, &parent_inode, &name));
  tracker.EndEnumerate(&cursor);

  EXPECT_EQ(2U, tracker.GetStatistics().num_insert);
  EXPECT_EQ(2U, tracker.GetStatistics().num_remove);
  EXPECT_EQ(3U, tracker.GetStatistics().num_prune);
}

}  // namespace glue
