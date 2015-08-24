/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include "env.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::AddGlobalTestEnvironment(new CvmfsEnvironment(argc, argv));
  return RUN_ALL_TESTS();
}
