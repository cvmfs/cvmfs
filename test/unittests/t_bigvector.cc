/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "bigvector.h"

class T_BigVector : public ::testing::Test {
 protected:
  virtual void SetUp() {
    vec_ = new BigVector<unsigned>();
    EXPECT_EQ(vec_->size(), (unsigned)0);
    EXPECT_GE(vec_->capacity(), vec_->size());
  }

  static const unsigned kNumSmall = 1000;
  static const unsigned kNumBig = 10000000;
  BigVector<unsigned> *vec_;
};


TEST_F(T_BigVector, SmallCycle) {
  unsigned N = kNumSmall;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  EXPECT_EQ(vec_->size(), N);
  EXPECT_GE(vec_->capacity(), vec_->size());

  for (unsigned i = 0; i < N; ++i) {
    unsigned value = vec_->At(i);
    EXPECT_EQ(value, i);
  }

  vec_->Clear();
  EXPECT_EQ(vec_->size(), (unsigned)0);
  EXPECT_LT(vec_->capacity(), (unsigned)100);
}


TEST_F(T_BigVector, BigCycle) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  EXPECT_EQ(vec_->size(), N);
  EXPECT_GE(vec_->capacity(), vec_->size());

  for (unsigned i = 0; i < N; ++i) {
    unsigned value = vec_->At(i);
    EXPECT_EQ(value, i);
  }

  vec_->Clear();
  EXPECT_EQ(vec_->size(), (unsigned)0);
  EXPECT_LT(vec_->capacity(), (unsigned)100);
}


TEST_F(T_BigVector, ShareBuffer) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  EXPECT_EQ(vec_->size(), N);
  EXPECT_GE(vec_->capacity(), vec_->size());

  unsigned *new_buf;
  bool large_alloc;
  vec_->ShareBuffer(&new_buf, &large_alloc);
  delete vec_;

  for (unsigned i = 0; i < N; ++i) {
    unsigned value = new_buf[i];
    EXPECT_EQ(value, i);
  }
}

TEST_F(T_BigVector, Copy) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  EXPECT_EQ(vec_->size(), N);
  EXPECT_GE(vec_->capacity(), vec_->size());

  BigVector<unsigned> new_vector(*vec_);
  for (unsigned i = 0; i < N; ++i) {
    unsigned value = new_vector.At(i);
    EXPECT_EQ(value, i);
  }

  new_vector = *vec_;
  for (unsigned i = 0; i < N; ++i) {
    unsigned value = new_vector.At(i);
    EXPECT_EQ(value, i);
  }
}

TEST_F(T_BigVector, Replace) {
  unsigned N = kNumSmall;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  for (unsigned i = 0; i < N; ++i) {
    vec_->Replace(i, 2 * i);
  }
  EXPECT_EQ(vec_->size(), N);
  EXPECT_GE(vec_->capacity(), vec_->size());

  for (unsigned i = 0; i < N; ++i) {
    unsigned value = vec_->At(i);
    EXPECT_EQ(value, 2 * i);
  }
}

TEST_F(T_BigVector, GrowShrink) {
  unsigned N = kNumBig;
  for (unsigned i = 0; i < N; ++i) {
    vec_->PushBack(i);
  }
  size_t old_capacity = vec_->capacity();
  vec_->SetSize(kNumBig - 1);
  vec_->ShrinkIfOversized();
  EXPECT_EQ(old_capacity, vec_->capacity());

  vec_->SetSize(100);
  vec_->ShrinkIfOversized();
  EXPECT_LT(vec_->capacity(), old_capacity);
  for (unsigned i = 0; i < 100; ++i) {
    unsigned value = vec_->At(i);
    EXPECT_EQ(value, i);
  }
}
