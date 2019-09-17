/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "bigqueue.h"

class T_BigQueue : public ::testing::Test {
 protected:
  virtual void SetUp() {
    queue_ = new BigQueue<unsigned>();
    EXPECT_EQ(0U, queue_->size());
    EXPECT_GE(queue_->capacity(), queue_->size());
  }

  virtual void TearDown() {
    delete queue_;
  }

  static const unsigned kNumSmall = 1000;
  static const unsigned kNumBig = 10000000;
  BigQueue<unsigned> *queue_;
};


TEST_F(T_BigQueue, SmallCycle) {
  unsigned N = kNumSmall;
  for (unsigned i = 0; i < N; ++i) {
    queue_->PushBack(i);
  }
  EXPECT_EQ(queue_->size(), N);
  EXPECT_GE(queue_->capacity(), queue_->size());

  unsigned *value = NULL;
  for (unsigned i = 0; i < N; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i);
    queue_->PopFront();
  }
  EXPECT_FALSE(queue_->Peek(&value));

  EXPECT_EQ(queue_->size(), (unsigned)0);
  EXPECT_LT(queue_->capacity(), (unsigned)100);
}

TEST_F(T_BigQueue, BigCycle) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    queue_->PushBack(i);
  }
  EXPECT_EQ(queue_->size(), N);
  EXPECT_GE(queue_->capacity(), queue_->size());

  unsigned *value = NULL;
  for (unsigned i = 0; i < N; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i);
    queue_->PopFront();
  }
  EXPECT_FALSE(queue_->Peek(&value));

  EXPECT_EQ(queue_->size(), (unsigned)0);
  EXPECT_LT(queue_->capacity(), (unsigned)100);
}
