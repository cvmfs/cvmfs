/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/prng.h"

class T_Prng : public ::testing::Test {
 protected:
  virtual void SetUp() {
    for (unsigned i = 0; i < buf_size; ++i) {
      buffer[i] = -1;
    }
    for (unsigned i = 0; i < rnd_range; ++i) {
      validation[i] = 0;
    }
    prng_ = new Prng();
  }

  virtual void TearDown() {
    delete prng_;
  }

 protected:
  static const uint32_t buf_size = 100000;
  static const uint32_t rnd_range = 100;
  int32_t buffer[buf_size];
  int32_t validation[rnd_range];
  Prng *prng_;
};


TEST_F(T_Prng, FullRange) {
  for (unsigned i = 0; i < buf_size; ++i) {
    uint32_t random_number = prng_->Next(rnd_range);
    validation[random_number] = 1;
    // printf("%d\n", random_number);
  }
  uint32_t test = 0;
  for (unsigned i = 0; i < rnd_range; ++i) {
    test += validation[i];
  }
  uint32_t compare = rnd_range;
  EXPECT_EQ(test, compare);
}


TEST_F(T_Prng, FullRangeTimeseed) {
  prng_->InitLocaltime();
  for (unsigned i = 0; i < buf_size; ++i) {
    uint32_t random_number = prng_->Next(rnd_range);
    validation[random_number] = 1;
    // printf("%d\n", random_number);
  }
  uint32_t test = 0;
  for (unsigned i = 0; i < rnd_range; ++i) {
    test += validation[i];
  }
  uint32_t compare = rnd_range;
  EXPECT_EQ(test, compare);
}


TEST_F(T_Prng, Deterministic) {
  prng_->InitSeed(7);
  for (unsigned i = 0; i < buf_size; ++i) {
    buffer[i] = prng_->Next(0xFFFFFFF);
  }
  prng_->InitSeed(7);
  for (unsigned i = 0; i < buf_size; ++i) {
    ASSERT_EQ(prng_->Next(0xFFFFFFF), (uint32_t)buffer[i]);
  }
}
