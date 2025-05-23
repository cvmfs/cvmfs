/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <signal.h>

#include "util/exception.h"
#include "util/logging.h"
#include "wpad.h"

using namespace std;  // NOLINT

class T_AssertOrLog : public ::testing::Test { };

#ifdef CVMFS_SUPPRESS_ASSERTS
TEST_F(T_AssertOrLog, LogError) {
  EXPECT_TRUE(AssertOrLog(1, kLogCvmfs, kLogDebug, ""));
  EXPECT_TRUE(AssertOrLog(2, kLogCvmfs, kLogDebug, ""));
  EXPECT_FALSE(AssertOrLog(0, kLogCvmfs, kLogDebug, ""));
}
#endif

#ifndef CVMFS_SUPPRESS_ASSERTS
TEST_F(T_AssertOrLog, Assert) {
  EXPECT_TRUE(AssertOrLog(1, kLogCvmfs, kLogDebug, ""));
  EXPECT_TRUE(AssertOrLog(2, kLogCvmfs, kLogDebug, ""));
  EXPECT_DEATH(AssertOrLog(0, kLogCvmfs, kLogDebug, ""),
               "Assertion");  // NOLINT
}
#endif
