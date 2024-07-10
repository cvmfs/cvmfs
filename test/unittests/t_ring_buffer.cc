/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstring>

#include "ring_buffer.h"
#include "util/pointer.h"

class T_RingBuffer : public ::testing::Test {
 protected:
  static const size_t kSize;

  virtual void SetUp() {
    ring = new RingBuffer(kSize);
  }

  virtual void TearDown() {
    ring.Destroy();
  }

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

  auto handle_null = ring->PushFront(NULL, 0);
  EXPECT_NE(RingBuffer::kInvalidObjectHandle, handle_null);
  EXPECT_EQ(0U, ring->GetObjectSize(handle_null));
  EXPECT_EQ(kSize - sizeof(size_t), ring->free_space());
}

TEST_F(T_RingBuffer, Wrap) {
  unsigned buf[kSize / 3 / sizeof(int)];
  memset(buf, 0, kSize / 3);

  RingBuffer::ObjectHandle_t objects[2];
  objects[0] = ring->PushFront(buf, kSize / 3);
  buf[0] = 1;
  objects[1] = ring->PushFront(buf, kSize / 3);

  for (unsigned i = 2; i < 1000; ++i) {
    EXPECT_EQ(kSize / 3, ring->GetObjectSize(objects[i % 2]));

    unsigned verify[kSize / 3 / sizeof(int)];
    ring->CopyObject(objects[i % 2], verify);

    EXPECT_EQ(2 * ((i - 2) / 2) + (i % 2), verify[0]);
    EXPECT_EQ(objects[i % 2], ring->RemoveBack());

    buf[0] = 2 * (i / 2) + (i % 2);
    objects[i % 2] = ring->PushFront(buf, kSize / 3);
  }
}
