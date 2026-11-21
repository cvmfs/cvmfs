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

  virtual void TearDown() { delete queue_; }

  static const unsigned kNumSmall = 1000;
  static const unsigned kNumBig = 10000000;
  static const unsigned kNumCycles = 300000;
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
  EXPECT_LT(queue_->capacity(), (unsigned)200);
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
  EXPECT_LT(queue_->capacity(), (unsigned)200);
}


TEST_F(T_BigQueue, JigSaw) {
  unsigned N = kNumSmall;
  for (unsigned i = 0; i < N / 2; ++i) {
    queue_->PushBack(i);
  }
  EXPECT_EQ(queue_->size(), N / 2);

  unsigned *value = NULL;
  for (unsigned i = 0; i < N / 4; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i);
    queue_->PopFront();
  }

  for (unsigned i = 0; i < N / 2; ++i) {
    queue_->PushBack(i);
  }
  EXPECT_EQ(queue_->size(), N - N / 4);

  for (unsigned i = 0; i < N / 4; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i + N / 4);
    queue_->PopFront();
  }
}


TEST_F(T_BigQueue, SmallLiveSetCycle) {
  // A queue that holds a single entry still advances head_ on every pop.  The
  // consumed prefix has to be reclaimed, otherwise the capacity grows with the
  // total number of pushes instead of the number of entries held.
  unsigned *value = NULL;
  queue_->PushBack(0);
  for (unsigned i = 1; i <= kNumCycles; ++i) {
    queue_->PushBack(i);
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i - 1);
    queue_->PopFront();
  }
  EXPECT_EQ(1U, queue_->size());
  EXPECT_LT(queue_->capacity(), (unsigned)200);
}


TEST_F(T_BigQueue, EmptyQueueCycle) {
  unsigned *value = NULL;
  for (unsigned i = 0; i < kNumCycles; ++i) {
    queue_->PushBack(i);
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i);
    queue_->PopFront();
  }
  EXPECT_EQ(0U, queue_->size());
  EXPECT_FALSE(queue_->Peek(&value));
  EXPECT_LT(queue_->capacity(), (unsigned)200);
}


TEST_F(T_BigQueue, LiveSetBoundary) {
  // The compaction threshold must not introduce a discontinuity: a queue
  // holding kCompactThreshold entries has to behave like one holding a few
  // more.  Capacity is expected to stay proportional to the live population.
  for (unsigned live = 62; live <= 66; ++live) {
    BigQueue<unsigned> queue;
    for (unsigned i = 0; i < live; ++i)
      queue.PushBack(i);
    for (unsigned i = 0; i < kNumCycles; ++i) {
      queue.PushBack(i);
      queue.PopFront();
    }
    EXPECT_EQ(live, queue.size());
    EXPECT_LT(queue.capacity(), 4 * live + 200);
  }
}


TEST_F(T_BigQueue, GrowingQueueHeadroom) {
  // Reclaiming the prefix must not come at the price of extra headroom for a
  // queue that keeps growing: two pushes per pop still has to settle close to
  // the live population rather than a multiple of it.
  const unsigned N = kNumSmall * 100;
  for (unsigned i = 0; i < N; ++i) {
    queue_->PushBack(i);
    queue_->PushBack(i);
    queue_->PopFront();
  }
  EXPECT_EQ(N, queue_->size());
  EXPECT_LT(queue_->capacity(), 2 * queue_->size());

  unsigned *value = NULL;
  for (unsigned i = 0; i < N; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    queue_->PopFront();
  }
  EXPECT_EQ(0U, queue_->size());
  EXPECT_LT(queue_->capacity(), (unsigned)200);
}


TEST_F(T_BigQueue, ReclaimAfterCycling) {
  for (unsigned i = 0; i < kNumCycles; ++i) {
    queue_->PushBack(i);
    queue_->PopFront();
  }
  for (unsigned i = 0; i < 5; ++i)
    queue_->PushBack(i);

  BigQueue<unsigned> copy(*queue_);
  EXPECT_EQ(5U, copy.size());
  EXPECT_LT(copy.capacity(), (unsigned)200);

  unsigned *value = NULL;
  for (unsigned i = 0; i < 5; ++i) {
    ASSERT_TRUE(copy.Peek(&value));
    EXPECT_EQ(i, *value);
    copy.PopFront();
  }

  queue_->Clear();
  EXPECT_EQ(0U, queue_->size());
  EXPECT_LT(queue_->capacity(), (unsigned)200);
}


TEST_F(T_BigQueue, Copy) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    queue_->PushBack(i);
  }
  EXPECT_EQ(queue_->size(), N);
  EXPECT_GE(queue_->capacity(), queue_->size());

  unsigned *value = NULL;
  for (unsigned i = 0; i < N / 2; ++i) {
    ASSERT_TRUE(queue_->Peek(&value));
    EXPECT_EQ(*value, i);
    queue_->PopFront();
  }
  EXPECT_EQ(N / 2, queue_->size());

  BigQueue<unsigned> new_queue(*queue_);
  EXPECT_EQ(queue_->size(), new_queue.size());
  EXPECT_EQ(queue_->size(), new_queue.capacity());
  for (unsigned i = 0; i < N / 2; ++i) {
    ASSERT_TRUE(new_queue.Peek(&value));
    EXPECT_EQ(i + N / 2, *value);
    new_queue.PopFront();
  }
  EXPECT_FALSE(new_queue.Peek(&value));

  EXPECT_EQ(0U, new_queue.size());
}
