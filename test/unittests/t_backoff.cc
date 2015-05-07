/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/backoff.h"

using namespace std;  // NOLINT

class T_Backoff : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  BackoffThrottle throttle_;
};


TEST_F(T_Backoff, Create) {
}
