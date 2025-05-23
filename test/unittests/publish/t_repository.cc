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
  virtual void SetUp() { }

 protected:
};


TEST_F(T_Repository, MasterReplica) {
  Publisher *publisher = GetTestPublisher();

  EXPECT_FALSE(publisher->IsMasterReplica());
  publisher->MarkReplicatible(true);
  EXPECT_TRUE(publisher->IsMasterReplica());
  publisher->MarkReplicatible(false);
  EXPECT_FALSE(publisher->IsMasterReplica());

  delete publisher;
}

}  // namespace publish
