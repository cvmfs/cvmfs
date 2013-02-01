/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FILE_CHUNKER_H_
#define CVMFS_UPLOAD_FILE_CHUNKER_H_

#include <utility>

#include "util.h"

namespace upload {

class Chunk : private std::pair<off_t, size_t> {
 public:
  Chunk(const off_t offset, const size_t size) :
    std::pair<off_t, size_t>(offset, size) {}

  inline off_t offset() const { return this->first; }
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
  off_t FindNextCutMark() const;

 private:
  const MemoryMappedFile &mmf_;

  off_t offset_;
};

}

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_H_ */
