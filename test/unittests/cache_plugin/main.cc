/**
 * This file is part of the CernVM File System.
 *
 * Tester for external cache plugins.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "globals.h"

std::string g_plugin_locator;

// TODO(jblomer): remove.  Only needed to satisfy monitor.cc
namespace cvmfs {
  pid_t pid_ = 0;
}

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
