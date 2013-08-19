#include <gtest/gtest.h>
#include <tbb/scalable_allocator.h>

#include "../../cvmfs/util.h"
#include "../../cvmfs/upload_file_processing/char_buffer.h"

TEST(T_Buffer, Initialize) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type> buffer;
  EXPECT_FALSE (buffer.IsInitialized());

  buffer.Allocate(100);
  EXPECT_TRUE (buffer.IsInitialized());
  EXPECT_NE (static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ (0u,   buffer.used_bytes());
  EXPECT_EQ (100u, buffer.size());

  buffer.SetUsedBytes(99);
  EXPECT_EQ (99u,  buffer.used_bytes());
  EXPECT_EQ (100u, buffer.size());

  buffer.SetUsed(40);
  EXPECT_EQ (40u,  buffer.used_bytes());
  EXPECT_EQ (40u,  buffer.used());
}

TEST(T_Buffer, InitializeByConstructor) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type> buffer(1024);
  EXPECT_TRUE (buffer.IsInitialized());
  EXPECT_NE (static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ (0u,    buffer.used_bytes());
  EXPECT_EQ (1024u, buffer.size());
}

TEST(T_Buffer, WriteAndRead) {
  typedef char buffer_type;
  const buffer_type *data = "abcdefghijklmnopqrs";

  Buffer<buffer_type> buffer(strlen(data) + 1);
  strncpy(buffer.ptr(), data, strlen(data) + 1);
  const std::string result = buffer.ptr();

  EXPECT_EQ (std::string(data), result);
  EXPECT_EQ (0u, buffer.used_bytes());

  buffer.SetUsedBytes(strlen(data) + 1);
  EXPECT_EQ (strlen(data) + 1, buffer.used_bytes());
}

TEST(T_Buffer, IntegerBuffer) {
  typedef uint32_t buffer_type;

  Buffer<buffer_type> buffer(1024);
  EXPECT_TRUE (buffer.IsInitialized());

  EXPECT_EQ (1024u,      buffer.size());
  EXPECT_EQ (1024u * 4u, buffer.size_bytes());
  EXPECT_EQ (0u,         buffer.used());
  EXPECT_EQ (0u,         buffer.used_bytes());

  buffer.SetUsed(30);
  EXPECT_EQ (30u,        buffer.used());
  EXPECT_EQ (30u * 4u,   buffer.used_bytes());

  buffer.SetUsedBytes(100);
  EXPECT_EQ (25u,        buffer.used());
  EXPECT_EQ (100u,       buffer.used_bytes());
}

TEST(T_Buffer, TbbScalableAllocator) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type, tbb::scalable_allocator<buffer_type> > buffer;
  EXPECT_FALSE (buffer.IsInitialized());

  buffer.Allocate(4096);
  EXPECT_TRUE (buffer.IsInitialized());
  EXPECT_NE (static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ (0u,    buffer.used_bytes());
  EXPECT_EQ (4096u, buffer.size());
}

TEST(T_Buffer, CharBuffer) {
  upload::CharBuffer buffer;
  EXPECT_FALSE (buffer.IsInitialized());
  EXPECT_EQ (0, buffer.base_offset());

  buffer.Allocate(100);
  EXPECT_TRUE (buffer.IsInitialized());
  EXPECT_NE (static_cast<unsigned char*>(0), buffer.ptr());
  EXPECT_EQ (0u,   buffer.used_bytes());
  EXPECT_EQ (100u, buffer.size());

  buffer.SetBaseOffset(1024);
  EXPECT_EQ (1024, buffer.base_offset());
}
