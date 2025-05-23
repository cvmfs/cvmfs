/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fence.h"

class T_Fence : public ::testing::Test {
 protected:
  virtual void SetUp() { }

  virtual void TearDown() { }

  Fence fence_;
};

TEST_F(T_Fence, Basics) {
  fence_.Close();
  EXPECT_EQ(1, fence_.blocking_);
  fence_.Open();
  EXPECT_EQ(0, fence_.blocking_);
  fence_.Enter();
  EXPECT_EQ(1, fence_.counter_);
  fence_.Leave();
  EXPECT_EQ(0, fence_.counter_);
  fence_.Drain();
  EXPECT_EQ(1, fence_.blocking_);
  fence_.Open();
  EXPECT_EQ(0, fence_.blocking_);

  {
    FenceGuard fence_guard(&fence_);
    EXPECT_EQ(1, fence_.counter_);
  }
  EXPECT_EQ(0, fence_.counter_);
}
