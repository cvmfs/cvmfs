/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/string.h"

const std::string kTestPath = "///ala/bala//";
const std::string kTestPathTrimmedLeading = "ala/bala//";
const std::string kTestPathTrimmedTrailing = "///ala/bala";
const std::string kTestPathTrimmedAll = "ala/bala";

class T_TrimString : public ::testing::Test { };

TEST_F(T_TrimString, TrimNone) {
  const std::string &trimmed = TrimString(kTestPath, "/", kTrimNone);
  ASSERT_EQ(kTestPath, trimmed);
}

TEST_F(T_TrimString, TrimLeading) {
  const std::string &trimmed = TrimString(kTestPath, "/", kTrimLeading);
  ASSERT_EQ(kTestPathTrimmedLeading, trimmed);
}

TEST_F(T_TrimString, TrimTrailing) {
  const std::string &trimmed = TrimString(kTestPath, "/", kTrimTrailing);
  ASSERT_EQ(kTestPathTrimmedTrailing, trimmed);
}

TEST_F(T_TrimString, TrimAll) {
  const std::string &trimmed = TrimString(kTestPath, "/", kTrimAll);
  ASSERT_EQ(kTestPathTrimmedAll, trimmed);
}
