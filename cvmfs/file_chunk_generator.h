/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_CHUNK_GENERATOR_H_
#define CVMFS_FILE_CHUNK_GENERATOR_H_

#include <utility>

#include "util.h"

namespace upload {

class Chunk : private std::pair<size_t, size_t> {
 public:
  Chunk(const size_t offset, const size_t size) :
    std::pair<size_t, size_t>(offset, size) {}

  inline size_t offset() const { return this->first; }
  inline size_t size() const   { return this->second; }
};

class ChunkGenerator : SingleCopy {
 public:

 public:
  ChunkGenerator(const MemoryMappedFile &mmf);
  virtual ~ChunkGenerator();

  Chunk Next();
  bool HasMoreData() const;

 protected:
  size_t FindNextCutMark() const;

 private:
  const MemoryMappedFile &mmf_;

  size_t offset_;
};

}

#endif /* CVMFS_FILE_CHUNK_GENERATOR_H_ */
