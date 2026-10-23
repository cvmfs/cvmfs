/**
 * This file is part of the CernVM File System.
 *
 * Minimal test main for mockfuse.
 */

#include <gtest/gtest.h>

#include "crypto/crypto_util.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Open /dev/[u]random before starting the unit tests to make sure that the
  // counting of open file descriptors is accurate
  crypto::InitRng();
  return RUN_ALL_TESTS();
}
