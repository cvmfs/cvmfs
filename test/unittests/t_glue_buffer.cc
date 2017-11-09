/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "glue_buffer.h"
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

}  // namespace glue
