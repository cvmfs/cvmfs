/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fuzztest/fuzztest.h"
#include "fuzztest/init_fuzztest.h"

#include <cassert>

#include "env.h"
#include "monitor.h"

int main(int argc, char **argv) {
  Watchdog *watchdog = Watchdog::Create(NULL, 0);
  assert(watchdog != NULL);
  // watchdog->Spawn();
  CvmfsEnvironment *env = new CvmfsEnvironment(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  // Registers the FUZZ_TEST-defined property tests with GoogleTest and parses
  // the FuzzTest command line flags. In unit-test mode each property runs as a
  // bounded GoogleTest case with randomized iterations.
  fuzztest::InitFuzzTest(&argc, &argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::AddGlobalTestEnvironment(env);
  int result = RUN_ALL_TESTS();
  delete watchdog;
  return result;
}
