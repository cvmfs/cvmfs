/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "ring_buffer.h"

class T_RingBuffer : public ::testing::Test {
 protected:
  virtual void SetUp() { }

  RingBuffer ring_buffer;
};

TEST_F(T_RingBuffer, Basics) {
}
