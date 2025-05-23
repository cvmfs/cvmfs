/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/file_backed_buffer.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

TEST(T_FileBackedBuffer, SimpleWriteAndRead) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(39);

  char source[] = "0123456789";
  buf->Append(source, 10);
  buf->Append(source, 10);
  buf->Append(source, 10);

  EXPECT_EQ(FileBackedBuffer::kWriteState, buf->state_);
  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(30ULL, buf->GetSize());

  buf->Append(source, 10);

  EXPECT_EQ(FileBackedBuffer::kWriteState, buf->state_);
  EXPECT_EQ(FileBackedBuffer::kFileMode, buf->mode_);
  EXPECT_EQ(40ULL, buf->GetSize());

  buf->Append(source, 10);
  buf->Commit();

  char *ptr;
  char *sink = reinterpret_cast<char *>(smalloc(100));

  EXPECT_EQ(FileBackedBuffer::kReadState, buf->state_);
  EXPECT_EQ(FileBackedBuffer::kFileMode, buf->mode_);
  EXPECT_EQ(50ULL, buf->GetSize());
  EXPECT_EQ(10LL, buf->Data(reinterpret_cast<void **>(&ptr), 10, 0));
  EXPECT_EQ(0, memcmp(source, ptr, 10));

  EXPECT_EQ(50LL, buf->Read(sink, 100));
  EXPECT_EQ(
      memcmp("01234567890123456789012345678901234567890123456789", sink, 50),
      0);
  free(sink);
  delete buf;
}

TEST(T_FileBackedBuffer, EmptyWrite) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(0);
  char source[] = "aa";
  buf->Append(source, 0);
  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(FileBackedBuffer::kWriteState, buf->state_);
  EXPECT_EQ(0ULL, buf->size_);
  EXPECT_EQ(0ULL, buf->pos_);
  buf->Commit();

  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(FileBackedBuffer::kReadState, buf->state_);
  EXPECT_EQ(0ULL, buf->size_);

  void *ptr;
  void *sink = smalloc(10);
  EXPECT_EQ(0LL, buf->Data(&ptr, 100, 0));
  EXPECT_EQ(0LL, buf->Read(sink, 10));

  delete buf;
  free(sink);
}

TEST(T_FileBackedBuffer, EmptyBuffer) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(0);

  buf->Commit();

  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(FileBackedBuffer::kReadState, buf->state_);
  EXPECT_EQ(0ULL, buf->size_);

  void *ptr;
  void *sink = smalloc(10);
  EXPECT_EQ(0LL, buf->Data(&ptr, 100, 0));
  EXPECT_EQ(0LL, buf->Read(sink, 10));

  free(sink);
  delete buf;
}

TEST(T_FileBackedBuffer, StraightToFile) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(0);

  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);

  char source[] = "abcde";
  buf->Append(source, 1);

  EXPECT_EQ(FileBackedBuffer::kFileMode, buf->mode_);
  EXPECT_EQ(1ULL, buf->size_);

  buf->Append(source + 1, 4);
  EXPECT_EQ(5ULL, buf->size_);

  buf->Commit();

  EXPECT_EQ(FileBackedBuffer::kFileMode, buf->mode_);
  EXPECT_EQ(FileBackedBuffer::kReadState, buf->state_);

  char *sink = reinterpret_cast<char *>(smalloc(10));
  char *ptr;
  EXPECT_EQ(5LL, buf->Read(sink, 10));
  EXPECT_EQ(5LL, buf->Data(reinterpret_cast<void **>(&ptr), 10, 0));
  EXPECT_EQ(0, memcmp(source, sink, 5));
  EXPECT_EQ(0, memcmp(source, ptr, 5));

  EXPECT_EQ(5ULL, buf->GetSize());

  free(sink);
  delete buf;
}

TEST(T_FileBackedBuffer, OnlyInMemory) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(40);

  char source[] = "0123456789";
  buf->Append(source, 10);
  buf->Append(source, 10);
  buf->Append(source, 10);
  buf->Append(source, 9);

  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(39ULL, buf->GetSize());

  buf->Commit();

  EXPECT_EQ(39ULL, buf->GetSize());
  EXPECT_EQ(FileBackedBuffer::kMemoryMode, buf->mode_);
  EXPECT_EQ(NULL, buf->mmapped_);

  void *ptr;
  EXPECT_EQ(39LL, buf->Data(&ptr, 100, 0));
  EXPECT_EQ(reinterpret_cast<void *>(buf->buf_), ptr);

  delete buf;
}

TEST(T_FileBackedBuffer, OutOfBoundsRead) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(40);

  char source[] = "0123456789";
  buf->Append(source, 10);

  buf->Commit();

  char *sink = reinterpret_cast<char *>(smalloc(20));
  EXPECT_EQ(10LL, buf->Read(sink, 20));
  EXPECT_EQ(0LL, buf->Read(sink, 20));
  EXPECT_EQ(0LL, buf->Read(sink, 20));

  free(sink);
  delete buf;
}
