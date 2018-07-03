/**
 * This file is part of the CernVM File System.
 *
 * Tester for external cache plugins.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "globals.h"


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // g_plugin_locator = argv[1];
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
