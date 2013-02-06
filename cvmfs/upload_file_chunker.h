/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FILE_CHUNKER_H_
#define CVMFS_UPLOAD_FILE_CHUNKER_H_

#include <utility>
#include <vector>
#include <cassert>

#include "util.h"

namespace upload {

class Chunk : private std::pair<off_t, size_t> {
 public:
  Chunk(const off_t offset, const size_t size) :
    std::pair<off_t, size_t>(offset, size) {}

  inline off_t offset() const { return this->first; }
  inline size_t size() const   { return this->second; }
};

class ChunkGenerator : public PolymorphicConstruction<ChunkGenerator,
                                                      MemoryMappedFile>,
                       private SingleCopy {
 public:
  ChunkGenerator(const MemoryMappedFile &mmf);
  virtual ~ChunkGenerator() {};

  Chunk Next();
  bool HasMoreData() const;

  static void RegisterPlugins();

 protected:
  inline const MemoryMappedFile& mmf()    const { return mmf_; }
  inline off_t                   offset() const { return offset_; }

  virtual off_t FindNextCutMark() const = 0;

  friend class AbstractSpooler;
  static void SetFileChunkRestrictions(const size_t minimal_chunk_size,
                                       const size_t average_chunk_size,
                                       const size_t maximal_chunk_size);

 protected:
  static size_t minimal_chunk_size_;
  static size_t average_chunk_size_;
  static size_t maximal_chunk_size_;

 private:

  const MemoryMappedFile           &mmf_;
  off_t                             offset_;
};

class NaiveChunkGenerator : public ChunkGenerator {
 public:
  NaiveChunkGenerator(const MemoryMappedFile &mmf) : ChunkGenerator(mmf) {};
  static bool  WillHandle(const MemoryMappedFile &mmf);

 protected:
  off_t        FindNextCutMark() const;
};

}

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_H_ */
