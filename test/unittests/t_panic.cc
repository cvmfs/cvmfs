/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <unistd.h>

#include "logging.h"
#include "util/exception.h"
#include "util/posix.h"

static void LogPanic(const LogSource source, const int mask, const char *msg)
{
  CreateFile("cvmfs_test_panic", 0600);
}

TEST(T_Panic, Call) {
  EXPECT_FALSE(FileExists("cvmfs_test_panic"));
  SetAltLogFunc(LogPanic);
  EXPECT_DEATH(PANIC(kLogStderr, "unit test"), ".*");
  SetAltLogFunc(NULL);
  EXPECT_TRUE(FileExists("cvmfs_test_panic"));
  unlink("cvmfs_test_panic");
}

TEST(T_Panic, CallNullPANIC) {
  EXPECT_FALSE(FileExists("cvmfs_test_panic"));
  SetAltLogFunc(LogPanic);
  EXPECT_DEATH(PANIC(NULL), ".*");
  SetAltLogFunc(NULL);
  EXPECT_TRUE(FileExists("cvmfs_test_panic"));
  unlink("cvmfs_test_panic");
}
