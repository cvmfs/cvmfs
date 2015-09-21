/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/bigvector.h"

class T_Bigvector : public ::testing::Test {
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


TEST_F(T_Bigvector, SmallCycle) {
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


TEST_F(T_Bigvector, BigCycle) {
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


TEST_F(T_Bigvector, ShareBuffer) {
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

TEST_F(T_Bigvector, Copy) {
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
