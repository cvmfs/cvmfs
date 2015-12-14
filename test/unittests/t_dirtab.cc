/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "../../cvmfs/path_filters/dirtab.h"

class T_Dirtab : public ::testing::Test {
 protected:
  catalog::Dirtab dirtab;
};

TEST_F(T_Dirtab, Initialize) {
  dirtab.Parse("");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(0u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ParseComment) {
  dirtab.Parse("# this is a comment\n"
               " #this as well.\n"
               " # and this also\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(0u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ParseBlankLines) {
  dirtab.Parse("  \n"
               "\n"
               "    \n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(0u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ParseSimpleRules) {
  dirtab.Parse("/lib/rule/*\n"
               "/lib/elur\n"
               "/lib/rule/??\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(3u, dirtab.RuleCount());
  EXPECT_EQ(3u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ParseNegativeRules) {
  dirtab.Parse("!/lib/rule/*\n"
               " !/lib/rule/.svn\n"
               " !  /lib/rule/??\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(3u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(3u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ParseDirtabWithCommentsAndBlankLines) {
  dirtab.Parse("# these are simple rules:\n"
               "/lib/rule/*\n"
               "/lib/elur\n"
               "\n"
               "# these as well:\n"
               "! /lib/rule/??\n"
               " ! */.git");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(4u, dirtab.RuleCount());
  EXPECT_EQ(2u, dirtab.PositiveRuleCount());
  EXPECT_EQ(2u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, MatchWithPositiveRulesOnly) {
  dirtab.Parse("# these are positive rules:\n"
               "/usr/*\n"
               "/usr/local/*\n"
               "\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(2u, dirtab.RuleCount());
  EXPECT_EQ(2u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());

  EXPECT_TRUE(dirtab.IsMatching("/usr/local"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/bin"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/lib"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/bin"));
  EXPECT_FALSE(dirtab.IsMatching("/usr"));
  EXPECT_FALSE(dirtab.IsMatching("usr"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/bin/x86"));
}


TEST_F(T_Dirtab, MatchWithNegativeRulesOnly) {
  dirtab.Parse("# these are negative rules:\n"
               "! /usr/*\n"
               "! /usr/local/*\n"
               "\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(2u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(2u, dirtab.NegativeRuleCount());

  EXPECT_FALSE(dirtab.IsMatching("/usr"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/lib"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local"));
  EXPECT_FALSE(dirtab.IsMatching("/var/spool"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/lib"));
}


TEST_F(T_Dirtab, MatchWithPositiveAndNegativeRules) {
  dirtab.Parse("# these are positive rules:\n"
               "/usr/*\n"
               "/usr/local/*\n"
               "\n"
               "# these are negative rules:\n"
               "! /usr/.svn\n"
               "! /usr/local/.git\n"
               "! /usr/local/.svn\n"
               " ! /usr/local/x86*\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(6u, dirtab.RuleCount());
  EXPECT_EQ(2u, dirtab.PositiveRuleCount());
  EXPECT_EQ(4u, dirtab.NegativeRuleCount());

  EXPECT_TRUE(dirtab.IsMatching("/usr/local"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/bin"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/svn"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/.git"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/lib"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/bin"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/git"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/local/svn"));
  EXPECT_FALSE(dirtab.IsMatching("/usr"));
  EXPECT_FALSE(dirtab.IsMatching("usr"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/bin/x86"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/.svn"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/.svn"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/.git"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/x86"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/local/x86_64"));
}


TEST_F(T_Dirtab, InvalidEscapedRule) {
  dirtab.Parse("# escaping a non-special character...\n"
               "! /usr/\\bin\n");

  EXPECT_FALSE(dirtab.IsValid());
  EXPECT_EQ(0u, dirtab.RuleCount());
  EXPECT_EQ(0u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, InvalidPositiveRule) {
  dirtab.Parse("# positive rules must be absolute!\n"
               "/usr/local\n"
               "hallo/welt.txt\n");

  EXPECT_FALSE(dirtab.IsValid());
  EXPECT_EQ(1u, dirtab.RuleCount());
  EXPECT_EQ(1u, dirtab.PositiveRuleCount());
  EXPECT_EQ(0u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, ContradictingPositiveAndNegativeRules) {
  dirtab.Parse("# positive:\n"
               "/usr/bin/*\n"
               "\n"
               "# negative:\n"
               "!/usr/bin/*");

  EXPECT_FALSE(dirtab.IsValid());
  EXPECT_EQ(2u, dirtab.RuleCount());
  EXPECT_EQ(1u, dirtab.PositiveRuleCount());
  EXPECT_EQ(1u, dirtab.NegativeRuleCount());
}


TEST_F(T_Dirtab, OpposingMatches) {
  dirtab.Parse("# positive:\n"
               "/usr/*\n"
               "/usr/local/*\n"
               "/foo/*\n"
               "\n"
               "# negative:\n"
               "!/usr/bi?\n"
               "!/foo/b*\n"
               "!/foo/.svn\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(6u, dirtab.RuleCount());
  EXPECT_EQ(3u, dirtab.PositiveRuleCount());
  EXPECT_EQ(3u, dirtab.NegativeRuleCount());

  EXPECT_TRUE(dirtab.IsOpposing("/usr/bin"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/bit"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/bip"));
  EXPECT_TRUE(dirtab.IsOpposing("/foo/bin"));
  EXPECT_TRUE(dirtab.IsOpposing("/foo/b"));
  EXPECT_TRUE(dirtab.IsOpposing("/foo/bar"));
  EXPECT_TRUE(dirtab.IsOpposing("/foo/ball"));
  EXPECT_TRUE(dirtab.IsOpposing("/foo/.svn"));
  EXPECT_FALSE(dirtab.IsOpposing("usr/bin"));
  EXPECT_FALSE(dirtab.IsOpposing("usr/bit"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/bi"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/binary"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/bi/"));
  EXPECT_FALSE(dirtab.IsOpposing("/foo/hallo"));
  EXPECT_FALSE(dirtab.IsOpposing("/foo/hallo/welt"));
  EXPECT_FALSE(dirtab.IsOpposing("/foo/hallo.welt"));
  EXPECT_FALSE(dirtab.IsOpposing("/foo/.git"));
}


TEST_F(T_Dirtab, MultiDirectoryWildcardsInNegativeRules) {
  dirtab.Parse("# positive:\n"
               "/usr/bin/*\n"
               "/usr/include/*\n"
               "\n"
               "# negative:\n"
               "! *.exe\n"
               "! */include/*.hpp\n");

  EXPECT_TRUE(dirtab.IsValid());
  EXPECT_EQ(4u, dirtab.RuleCount());
  EXPECT_EQ(2u, dirtab.PositiveRuleCount());
  EXPECT_EQ(2u, dirtab.NegativeRuleCount());

  EXPECT_TRUE(dirtab.IsMatching("/usr/bin/bash"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/include/stdio.h"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/include/"));
  EXPECT_TRUE(dirtab.IsMatching("/usr/bin/root.exe.bak"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/include/random.hpp"));
  EXPECT_FALSE(dirtab.IsMatching("/usr/bin/root.exe"));

  EXPECT_TRUE(dirtab.IsOpposing("/usr/bin/bash.exe"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/bin/foo.exe"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/local/include/bar.exe"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/include/foo/bar.exe"));
  EXPECT_TRUE(dirtab.IsOpposing("/usr/include/foo/bar.exe"));
  EXPECT_TRUE(dirtab.IsOpposing("/root/include/stdio.hpp"));
  EXPECT_TRUE(dirtab.IsOpposing("/root/include/tr1/stdio.hpp"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/bin/sh"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/include/stdio.h"));
  EXPECT_FALSE(dirtab.IsOpposing("/var/log/messages"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/local/src/test.h"));
  EXPECT_FALSE(dirtab.IsOpposing("/usr/local/src/test.hpp"));
}
