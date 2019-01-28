/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <openssl/rand.h>

#include <cassert>

#include "env.h"
#include "monitor.h"

int main(int argc, char **argv) {
  Watchdog *watchdog = Watchdog::Create("/tmp/stacktrace.cvmfs_unittests");
  assert(watchdog != NULL);
  //  watchdog->Spawn();
  CvmfsEnvironment* env = new CvmfsEnvironment(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::AddGlobalTestEnvironment(env);
  // Open /dev/[u]random before starting the unit tests to make sure that the
  // counting of open file descriptors is accurate
  RAND_poll();
  int result = RUN_ALL_TESTS();
  delete watchdog;
  return result;
}
