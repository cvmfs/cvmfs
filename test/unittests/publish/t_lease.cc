/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "c_repository.h"
#include "publish/repository.h"
#include "publish/settings.h"

using namespace std;  // NOLINT

namespace publish {

class T_Repository : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

 protected:
};


TEST_F(T_Lease, Acquire) {
  Publisher *publisher = GetTestPublisherGateway(1234);

  delete publisher;
}

}  // namespace publish
