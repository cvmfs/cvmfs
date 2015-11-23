/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/platform.h"

TEST(T_Platforms, OsVersion) {
  int major = -1;
  int minor = -1;
  int patch = -1;
  platform_get_os_version(&major, &minor, &patch);

  EXPECT_GT(major, 0);
  EXPECT_GE(minor, 0);
  EXPECT_GE(patch, 0);
}
