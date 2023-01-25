/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <signal.h>

#include "util/exception.h"
#include "util/logging.h"

#include "wpad.h"

using namespace std;  // NOLINT

class T_Conditional_Assert : public ::testing::Test {};

#ifdef CVMFS_SUPPRESS_ASSERTS
TEST_F(T_Conditional_Assert, LogError) {
  EXPECT_TRUE(assert_or_log_error(1, kLogCvmfs, kLogDebug, ""));
  EXPECT_TRUE(assert_or_log_error(2, kLogCvmfs, kLogDebug, ""));
  EXPECT_FALSE(assert_or_log_error(0, kLogCvmfs, kLogDebug, ""));
}
#endif

#ifndef CVMFS_SUPPRESS_ASSERTS
TEST_F(T_Conditional_Assert, Assert) {
  EXPECT_TRUE(assert_or_log_error(1, kLogCvmfs, kLogDebug, ""));
  EXPECT_TRUE(assert_or_log_error(2, kLogCvmfs, kLogDebug, ""));
  #ifdef __APPLE__
    EXPECT_DEATH(assert_or_log_error(0, kLogCvmfs, kLogDebug, ""),
          "Assertion failed: (t), function assert_or_log_error, "
          "file exception.h, line 56.");
  #else
    EXPECT_DEATH(assert_or_log_error(0, kLogCvmfs, kLogDebug, ""),
                                    "Assertion `t' failed.");
  #endif
}
#endif
