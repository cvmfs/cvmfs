/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstring>

#include "ring_buffer.h"
#include "util/pointer.h"
#include "util/prng.h"

class T_RingBuffer : public ::testing::Test {
 protected:
  static const size_t kSize;

  virtual void SetUp() { ring = new RingBuffer(kSize); }

  virtual void TearDown() { ring.Destroy(); }

  UniquePtr<RingBuffer> ring;
};

const size_t T_RingBuffer::kSize = 997;

TEST_F(T_RingBuffer, Basics) {
  unsigned char buf[kSize];
  memset(buf, 0, kSize);

  EXPECT_EQ(RingBuffer::kInvalidObjectHandle, ring->PushFront(buf, kSize));
  EXPECT_EQ(0u, ring->PushFront(buf, kSize - sizeof(size_t)));
  EXPECT_EQ(0u, ring->free_space());
  EXPECT_EQ(0u, ring->RemoveBack());
  EXPECT_EQ(kSize, ring->free_space());

  const RingBuffer::ObjectHandle_t handle_null = ring->PushFront(NULL, 0);
  EXPECT_NE(RingBuffer::kInvalidObjectHandle, handle_null);
  EXPECT_EQ(0U, ring->GetObjectSize(handle_null));
  EXPECT_EQ(kSize - sizeof(size_t), ring->free_space());
}

TEST_F(T_RingBuffer, Wrap) {
  Prng prng;
  prng.InitSeed(137);

  const size_t N = kSize / 3 / sizeof(unsigned);

  unsigned buf[N];
  for (unsigned i = 0; i < N; ++i) {
    buf[i] = i;
  }

  RingBuffer::ObjectHandle_t objects[2];
  objects[0] = ring->PushFront(buf, kSize / 3);
  buf[0] = 1;
  objects[1] = ring->PushFront(buf, kSize / 3);

  for (unsigned i = 2; i < 1000; ++i) {
    EXPECT_EQ(kSize / 3, ring->GetObjectSize(objects[i % 2]));

    unsigned verify[N];
    ring->CopyObject(objects[i % 2], verify);
    EXPECT_EQ(2 * ((i - 2) / 2) + (i % 2), verify[0]);
    for (unsigned j = 0; j < 100; ++j) {
      unsigned begin = prng.Next(N);
      const unsigned num = prng.Next(N - begin + 1);
      ring->CopySlice(objects[i % 2], num * sizeof(unsigned),
                      begin * sizeof(unsigned), verify);
      const size_t offset = begin;
      while (begin < num) {
        if (begin != 0) {
          EXPECT_EQ(begin, verify[begin - offset]);
        }
        begin++;
      }
    }

    EXPECT_EQ(objects[i % 2], ring->RemoveBack());

    buf[0] = 2 * (i / 2) + (i % 2);
    objects[i % 2] = ring->PushFront(buf, kSize / 3);
  }
}
