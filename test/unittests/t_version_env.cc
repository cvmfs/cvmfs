/**
 * This file is part of the CernVM File System.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "mountpoint.h"

using namespace std;  // NOLINT

class T_VersionEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // Clear any existing environment variables
    unsetenv("CVMFS_VERSION");
    unsetenv("CVMFS_VERSION_NUMERIC");
    unsetenv("CVMFS_ARCH");
  }
};

TEST_F(T_VersionEnv, SetupGlobalEnvironmentParams) {
  // Call the function to set up environment variables
  FileSystem::SetupGlobalEnvironmentParams();

  // Check CVMFS_VERSION is set
  const char *cvmfs_version = getenv("CVMFS_VERSION");
  ASSERT_NE(cvmfs_version, nullptr) << "CVMFS_VERSION should be set";
  EXPECT_STREQ(cvmfs_version, CVMFS_VERSION)
      << "CVMFS_VERSION should match compile-time constant";

  // Check CVMFS_VERSION_NUMERIC is set and calculated correctly
  const char *cvmfs_version_numeric = getenv("CVMFS_VERSION_NUMERIC");
  ASSERT_NE(cvmfs_version_numeric, nullptr)
      << "CVMFS_VERSION_NUMERIC should be set";

  int numeric_version = atoi(cvmfs_version_numeric);
  int expected_numeric = CVMFS_VERSION_MAJOR * 10000 + CVMFS_VERSION_MINOR * 100
                         + CVMFS_VERSION_PATCH;
  EXPECT_EQ(numeric_version, expected_numeric)
      << "CVMFS_VERSION_NUMERIC should be calculated as major*10000 + "
         "minor*100 + patch";

  // For version 2.13.2, this should be 21302
  EXPECT_EQ(numeric_version, 21302)
      << "For version 2.13.2, numeric should be 21302";

  // Check CVMFS_ARCH is set (this was already working)
  const char *cvmfs_arch = getenv("CVMFS_ARCH");
  ASSERT_NE(cvmfs_arch, nullptr) << "CVMFS_ARCH should be set";
  EXPECT_GT(strlen(cvmfs_arch), 0) << "CVMFS_ARCH should not be empty";
}

TEST_F(T_VersionEnv, VersionNumericFormat) {
  // Test the numeric format calculation directly
  int test_cases[][4] = {// major, minor, patch, expected_numeric
                         {2, 13, 2, 21302},
                         {3, 0, 0, 30000},
                         {2, 15, 10, 21510},
                         {1, 2, 3, 10203}};

  for (size_t i = 0; i < sizeof(test_cases) / sizeof(test_cases[0]); ++i) {
    int major = test_cases[i][0];
    int minor = test_cases[i][1];
    int patch = test_cases[i][2];
    int expected = test_cases[i][3];

    int calculated = major * 10000 + minor * 100 + patch;
    EXPECT_EQ(calculated, expected)
        << "For version " << major << "." << minor << "." << patch
        << ", expected " << expected << " but got " << calculated;
  }
}
