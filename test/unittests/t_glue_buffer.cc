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
  uint64_t inode = 0;
  NameString name;
  InodeTracker::Cursor cursor = inode_tracker_.BeginEnumerate();
  EXPECT_FALSE(inode_tracker_.Next(&cursor, &inode, &name));
  EXPECT_FALSE(inode_tracker_.Next(&cursor, &inode, &name));
  inode_tracker_.EndEnumerate(&cursor);

  inode_tracker_.VfsGet(1, PathString(""));
  inode_tracker_.VfsGet(2, PathString("/foo"));
  inode_tracker_.VfsGet(4, PathString("/foo/bar"));
  cursor = inode_tracker_.BeginEnumerate();
  int bitset = 0;
  for (unsigned i = 0; i < 3; ++i) {
    EXPECT_TRUE(inode_tracker_.Next(&cursor, &inode, &name));
    switch (inode) {
      case 0:
        EXPECT_EQ("", name.ToString());
        bitset |= 1;
        break;
      case 1:
        EXPECT_EQ("foo", name.ToString());
        bitset |= 2;
        break;
      case 2:
        EXPECT_EQ("bar", name.ToString());
        bitset |= 4;
        break;
      default:
        EXPECT_FALSE(true);
    }
  }
  EXPECT_EQ(7, bitset);
  EXPECT_FALSE(inode_tracker_.Next(&cursor, &inode, &name));
  inode_tracker_.EndEnumerate(&cursor);
}

}  // namespace glue
