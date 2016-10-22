/**
 * This file is part of the CernVM File System.
 *
 * Tester for external cache plugins.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "globals.h"

std::string g_plugin_locator;


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc != 2) {
    fprintf(stderr, "cache plugin locator missing\n");
    return 1;
  }
  g_plugin_locator = argv[1];
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
