/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_FILE_BACKED_BUFFER_H_
#define CVMFS_UTIL_FILE_BACKED_BUFFER_H_

#include <stdint.h>

#include <string>

#include "gtest/gtest_prod.h"
#include "util/export.h"
#include "util/mmap_file.h"
#include "util/single_copy.h"

/**
 * FileBackedBuffer is a class for smart data writing and reading.
 * It represents a buffer that is in a first step filled with data and,
 * once all data has been written, it can be read back out.
 *
 * It works in 2 modes: memory mode and file mode. It first starts
 * working in memory mode, but as soon as the buffer size reaches
 * a configurable threshold, it writes all data into a temporary file,
 * freeing up the occupied memory. From that point, all further data
 * are written directly into the file.
 *
 * Reading is done straight from memory or using mmap, depending on the
 * current mode of operation.
 *
 * Method call timeline:
 * 1. Create
 * 2. Append
 * 3. Commit
 * 4. Data/Read/ReadP, Rewind
 */
class CVMFS_EXPORT FileBackedBuffer : SingleCopy {
  FRIEND_TEST(T_FileBackedBuffer, SimpleWriteAndRead);
  FRIEND_TEST(T_FileBackedBuffer, EmptyBuffer);
  FRIEND_TEST(T_FileBackedBuffer, EmptyWrite);
  FRIEND_TEST(T_FileBackedBuffer, StraightToFile);
  FRIEND_TEST(T_FileBackedBuffer, OnlyInMemory);

 public:
  static FileBackedBuffer *Create(uint64_t in_memory_threshold,
                                  const std::string &tmp_dir = "/tmp/");

  ~FileBackedBuffer();
  void Append(const void *source, uint64_t len);
  void Commit();
  int64_t Data(void **ptr, int64_t len, uint64_t pos);
  int64_t Read(void *ptr, int64_t len);
  int64_t ReadP(void *ptr, int64_t len, uint64_t pos);
  void Rewind();

  uint64_t GetSize() const;

 private:
  FileBackedBuffer(uint64_t in_memory_threshold, const std::string &tmp_dir);
  void SaveToFile();

  const uint64_t in_memory_threshold_;
  const std::string tmp_dir_;

  enum {
    kWriteState = 0,
    kReadState
  } state_;

  enum {
    kMemoryMode = 0,
    kFileMode
  } mode_;

  uint64_t size_;
  // used for writing and reading in kMemoryMode
  unsigned char *buf_;
  // used for reading and writing in both modes
  uint64_t pos_;
  // used for writing in kFileMode
  FILE *fp_;
  // used for writing and reading (opened mmapped_) in kFileMode
  std::string file_path_;
  // used for reading in kFileMode
  MemoryMappedFile *mmapped_;
};

#endif  // CVMFS_UTIL_FILE_BACKED_BUFFER_H_
