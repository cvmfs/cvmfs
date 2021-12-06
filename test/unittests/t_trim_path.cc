/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/posix.h"

const std::string kTestPath = "///ala/bala//";
const std::string kTestPathTrimmedLeading = "ala/bala//";
const std::string kTestPathTrimmedTrailing = "///ala/bala";
const std::string kTestPathTrimmedAll = "ala/bala";

class T_TrimPath : public ::testing::Test {
};

TEST_F(T_TrimPath, TrimNone) {
  const std::string& trimmed = TrimPath(kTestPath, "/", kTrimNone);
  ASSERT_EQ(kTestPath, trimmed);
}

TEST_F(T_TrimPath, TrimLeading) {
  const std::string& trimmed = TrimPath(kTestPath, "/", kTrimLeading);
  ASSERT_EQ(kTestPathTrimmedLeading, trimmed);
}

TEST_F(T_TrimPath, TrimTrailing) {
  const std::string& trimmed = TrimPath(kTestPath, "/", kTrimTrailing);
  ASSERT_EQ(kTestPathTrimmedTrailing, trimmed);
}

TEST_F(T_TrimPath, TrimAll) {
  const std::string& trimmed = TrimPath(kTestPath, "/", kTrimAll);
  ASSERT_EQ(kTestPathTrimmedAll, trimmed);
}
