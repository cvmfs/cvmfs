/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include "env.h"

int main(int argc, char **argv) {
  CvmfsEnvironment* env = new CvmfsEnvironment(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::AddGlobalTestEnvironment(env);
  return RUN_ALL_TESTS();
}
