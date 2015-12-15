/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "../../cvmfs/path_filters/relaxed_path_filter.h"

class T_RelaxedPathFilter : public ::testing::Test {
 protected:
  catalog::RelaxedPathFilter path_filter;
};


TEST_F(T_RelaxedPathFilter, RelaxedPathFilter) {
  path_filter.Parse("# positive\n"
                    "/usr/include/*\n"
                    "/bin/bash\n"
                    "# negatives\n"
                    "! /usr/local/bin/*\n");

  // here the RelaxedPathFilter will positively match the following:
  // /usr, /usr/include, /usr/include/*
  // /bin, /bin/bash
  EXPECT_EQ(6u, path_filter.RuleCount());
  EXPECT_EQ(5u, path_filter.PositiveRuleCount());
  EXPECT_EQ(1u, path_filter.NegativeRuleCount());

  EXPECT_TRUE(path_filter.IsMatching("/bin/bash"));
  EXPECT_TRUE(path_filter.IsMatching("/bin"));
  EXPECT_FALSE(path_filter.IsMatching("/bin/foo"));
  EXPECT_TRUE(path_filter.IsMatching("/usr/include/stdio.h"));
  EXPECT_TRUE(path_filter.IsMatching("/usr/include"));
  EXPECT_TRUE(path_filter.IsMatching("/usr"));
  EXPECT_FALSE(path_filter.IsMatching("/"));
  EXPECT_FALSE(path_filter.IsMatching(""));

  EXPECT_TRUE(path_filter.IsOpposing("/usr/local/bin/foo"));
  EXPECT_FALSE(path_filter.IsOpposing("/usr/local/bin"));
  EXPECT_FALSE(path_filter.IsOpposing("/usr/local"));
}


TEST_F(T_RelaxedPathFilter, RelaxedPathFilterSubtrees) {
  path_filter.Parse("# positive\n"
                    "/software/releases\n"
                    "# negatives\n"
                    "! /software/releases/misc\n"
                    "! /software/releases/experimental/misc\n");

  EXPECT_TRUE(path_filter.IsValid());

  EXPECT_TRUE(path_filter.IsMatching("/software"));
  EXPECT_TRUE(path_filter.IsMatching("/software/releases"));
  EXPECT_TRUE(path_filter.IsMatching("/software/releases/v1"));
  EXPECT_TRUE(path_filter.IsMatching("/software/releases/experimental"));

  EXPECT_FALSE(path_filter.IsMatching("/software/apps"));
  EXPECT_FALSE(path_filter.IsMatching("/software/releases/misc"));
  EXPECT_FALSE(path_filter.IsMatching("/software/releases/misc/external"));
  EXPECT_FALSE(path_filter.IsMatching("/software/releases/experimental/misc"));
  EXPECT_FALSE(
    path_filter.IsMatching("/software/releases/experimental/misc/foo"));
}


TEST_F(T_RelaxedPathFilter, RelaxedPathFilterTrailingSlash) {
  path_filter.Parse("# positive:\n"
                    "/usr/bin/\n"
                    "# negative:\n"
                    "! *.exe/\n"
                    "! /usr/misc/\n");

  EXPECT_TRUE(path_filter.IsValid());

  EXPECT_TRUE(path_filter.IsMatching("/usr/bin"));
  EXPECT_TRUE(path_filter.IsMatching("/usr/bin/bash"));
  EXPECT_FALSE(path_filter.IsMatching("/usr/misc"));
  EXPECT_FALSE(path_filter.IsMatching("/usr/bin/root.exe"));
}
