/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/quota.h"

using namespace std;  // NOLINT

class T_QuotaManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

 protected:
  PosixQuotaManager *quota_mgr_;
};
