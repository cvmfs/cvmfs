/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <tbb/scalable_allocator.h>

#include "../../cvmfs/file_processing/char_buffer.h"
#include "../../cvmfs/util.h"


TEST(T_Buffer, Initialize) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type> buffer;
  EXPECT_FALSE(buffer.IsInitialized());

  buffer.Allocate(100);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_NE(static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ(0u,   buffer.used_bytes());
  EXPECT_EQ(100u, buffer.size());
  EXPECT_EQ(100u, buffer.free());

  buffer.SetUsedBytes(99);
  EXPECT_EQ(99u,  buffer.used_bytes());
  EXPECT_EQ(100u, buffer.size());
  EXPECT_EQ(1u,   buffer.free());

  buffer.SetUsed(40);
  EXPECT_EQ(40u,  buffer.used_bytes());
  EXPECT_EQ(40u,  buffer.used());
  EXPECT_EQ(60u,  buffer.free());
}

TEST(T_Buffer, InitializeByConstructor) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type> buffer(1024);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_NE(static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ(0u,    buffer.used_bytes());
  EXPECT_EQ(1024u, buffer.size());
  EXPECT_EQ(1024u, buffer.free());
}

TEST(T_Buffer, WriteAndRead) {
  typedef char buffer_type;
  const buffer_type *data = "abcdefghijklmnopqrs";
  const size_t       str_length = strlen(data) + 1;

  Buffer<buffer_type> buffer(str_length);
  strncpy(buffer.ptr(), data, str_length);
  const std::string result = buffer.ptr();

  EXPECT_EQ(std::string(data), result);
  EXPECT_EQ(0u,                buffer.used_bytes());
  EXPECT_EQ(str_length,        buffer.free());

  buffer.SetUsedBytes(str_length);
  EXPECT_EQ(str_length, buffer.used_bytes());
  EXPECT_EQ(0u,         buffer.free());
}

TEST(T_Buffer, IntegerBuffer) {
  typedef uint32_t buffer_type;

  Buffer<buffer_type> buffer(1024);
  EXPECT_TRUE(buffer.IsInitialized());

  EXPECT_EQ(1024u,      buffer.size());
  EXPECT_EQ(1024u * 4u, buffer.size_bytes());
  EXPECT_EQ(1024u,      buffer.free());
  EXPECT_EQ(1024u * 4u, buffer.free_bytes());
  EXPECT_EQ(0u,         buffer.used());
  EXPECT_EQ(0u,         buffer.used_bytes());

  buffer.SetUsed(30);
  EXPECT_EQ(30u,          buffer.used());
  EXPECT_EQ(30u * 4u,     buffer.used_bytes());
  EXPECT_EQ(1024u - 30u,  buffer.free());
  EXPECT_EQ(4096u - 120u, buffer.free_bytes());

  buffer.SetUsedBytes(100);
  EXPECT_EQ(25u,          buffer.used());
  EXPECT_EQ(100u,         buffer.used_bytes());
  EXPECT_EQ(1024u -  25u, buffer.free());
  EXPECT_EQ(4096u - 100u, buffer.free_bytes());
}

TEST(T_Buffer, TbbScalableAllocator) {
  typedef unsigned char buffer_type;

  Buffer<buffer_type, tbb::scalable_allocator<buffer_type> > buffer;
  EXPECT_FALSE(buffer.IsInitialized());

  buffer.Allocate(4096);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_NE(static_cast<buffer_type*>(0), buffer.ptr());
  EXPECT_EQ(0u,    buffer.used_bytes());
  EXPECT_EQ(4096u, buffer.size());
  EXPECT_EQ(4096u, buffer.free());
}

TEST(T_Buffer, CharBuffer) {
  upload::CharBuffer buffer;
  EXPECT_FALSE(buffer.IsInitialized());
  EXPECT_EQ(0, buffer.base_offset());

  buffer.Allocate(100);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_NE(static_cast<unsigned char*>(0), buffer.ptr());
  EXPECT_EQ(0u,   buffer.used_bytes());
  EXPECT_EQ(100u, buffer.size());
  EXPECT_EQ(100u, buffer.free());

  buffer.SetBaseOffset(1024);
  EXPECT_EQ(1024, buffer.base_offset());
  EXPECT_EQ(100u, buffer.size());
  EXPECT_EQ(100u, buffer.free());
}

TEST(T_Buffer, FreeSpacePointer) {
  upload::CharBuffer buffer;
  EXPECT_FALSE(buffer.IsInitialized());
  EXPECT_EQ(0, buffer.base_offset());

  const char *str = "This is a simple test!";
  const size_t str_length = strlen(str) + 1;
  buffer.Allocate(str_length);
  memset(buffer.ptr(), 0, str_length);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_EQ(str_length, buffer.free_bytes());

  const off_t test_off = 10;
  memcpy(buffer.ptr(), str, test_off);
  EXPECT_EQ(str_length, buffer.free_bytes());
  EXPECT_NE(std::string(str),
            std::string(reinterpret_cast<const char*>(buffer.ptr())));
  EXPECT_EQ(std::string("This is a "),
            std::string(reinterpret_cast<const char*>(buffer.ptr())));

  buffer.SetUsedBytes(test_off);
  EXPECT_EQ(static_cast<size_t>(test_off), buffer.used_bytes());
  EXPECT_EQ(str_length - test_off,         buffer.free_bytes());
  EXPECT_EQ(buffer.ptr() + test_off,       buffer.free_space_ptr());

  memcpy(buffer.free_space_ptr(), str + test_off, str_length - test_off);
  EXPECT_EQ(std::string(str),
            std::string(reinterpret_cast<const char*>(buffer.ptr())));
  EXPECT_NE(str_length, buffer.used_bytes());
}

TEST(T_Buffer, CloneCharBuffer) {
  upload::CharBuffer buffer;
  EXPECT_FALSE(buffer.IsInitialized());
  EXPECT_EQ(0, buffer.base_offset());

  const char *str = "This is a simple test!";
  const size_t str_length = strlen(str) + 1;
  buffer.Allocate(str_length);
  EXPECT_TRUE(buffer.IsInitialized());
  EXPECT_EQ(str_length, buffer.free_bytes());

  memcpy(buffer.ptr(), str, str_length);
  buffer.SetUsedBytes(str_length);
  EXPECT_EQ(str_length,                buffer.used_bytes());
  EXPECT_EQ(0u,                        buffer.free_bytes());
  EXPECT_EQ(buffer.ptr() + str_length, buffer.free_space_ptr());
  EXPECT_EQ(std::string(str),
            std::string(reinterpret_cast<const char*>(buffer.ptr())));


  const off_t test_base_off = 128;
  buffer.SetBaseOffset(test_base_off);
  EXPECT_EQ(test_base_off, buffer.base_offset());

  upload::CharBuffer *other = buffer.Clone();
  EXPECT_EQ(std::string(str),
            std::string(reinterpret_cast<const char*>(other->ptr())));
  EXPECT_EQ(0u,            other->free());
  EXPECT_EQ(str_length,    other->size());
  EXPECT_EQ(str_length,    other->used_bytes());
  EXPECT_EQ(test_base_off, other->base_offset());
}
