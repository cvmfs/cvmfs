/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_suid_util.h"
#include "gtest/gtest.h"

using namespace std;  // NOLINT

namespace cvmfs_suid {

TEST(T_SuidUtil, EscapeSystemdUnit) {
  EXPECT_DEATH(EscapeSystemdUnit(""), ".*");

  EXPECT_EQ("-.mount", EscapeSystemdUnit("/"));
  EXPECT_EQ("-.mount", EscapeSystemdUnit("//"));
  EXPECT_EQ("-.mount", EscapeSystemdUnit("///"));
  EXPECT_EQ("-.mount", EscapeSystemdUnit("////"));

  EXPECT_EQ("a-b-c.mount", EscapeSystemdUnit("/a/b/c"));
  EXPECT_EQ("a-b-c.mount", EscapeSystemdUnit("/a/b//c"));

  EXPECT_EQ("a-b-c.mount", EscapeSystemdUnit("/a/b/c/"));
  EXPECT_EQ("\\x2ea_-.b-c\\x40.mount", EscapeSystemdUnit(".a_/.b/c@"));
}


TEST(T_SuidUtil, PathExists) {
  EXPECT_TRUE(PathExists("."));
  EXPECT_FALSE(PathExists("/no/such/path"));
}


TEST(T_SuidUtil, ResolvePath) {
  EXPECT_EQ("/usr", ResolvePath("/usr"));
  EXPECT_EQ("/usr", ResolvePath("/usr/"));
  EXPECT_EQ("/usr", ResolvePath("/usr/../usr/./"));
}


}  // namespace cvmfs_suid
