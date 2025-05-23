/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cassert>

#include "crypto/crypto_util.h"
#include "env.h"
#include "monitor.h"

int main(int argc, char **argv) {
  Watchdog *watchdog = Watchdog::Create(NULL);
  assert(watchdog != NULL);
  //  watchdog->Spawn("/tmp/stacktrace.cvmfs_unittests");
  CvmfsEnvironment *env = new CvmfsEnvironment(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::AddGlobalTestEnvironment(env);
  // Open /dev/[u]random before starting the unit tests to make sure that the
  // counting of open file descriptors is accurate
  crypto::InitRng();
  int result = RUN_ALL_TESTS();
  delete watchdog;
  return result;
}
