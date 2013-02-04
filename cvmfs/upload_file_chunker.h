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

class ChunkGenerator;

class ChunkGeneratorFactory {
 protected:
  ChunkGeneratorFactory() {}

  friend class ChunkGenerator;
  virtual bool WillHandleFile(const MemoryMappedFile &mmf) const = 0;
  virtual ChunkGenerator* Construct(const MemoryMappedFile &mmf) const = 0;
};

template <class ChunkGeneratorT>
class ChunkGeneratorFactoryImpl : public ChunkGeneratorFactory {
 protected:
  inline bool WillHandleFile(const MemoryMappedFile &mmf) const {
    return ChunkGeneratorT::WillHandleFile(mmf);
  }

  inline ChunkGenerator* Construct(const MemoryMappedFile &mmf) const;
};

class ChunkGenerator : SingleCopy {
 public:
  static ChunkGenerator* Construct(const MemoryMappedFile &mmf);

 public:
  virtual ~ChunkGenerator() {};

  Chunk Next();
  bool HasMoreData() const;

 protected:
  friend class ChunkGeneratorFactory;
  ChunkGenerator(const MemoryMappedFile &mmf);

  inline const MemoryMappedFile& mmf()    const { return mmf_; }
  inline off_t                   offset() const { return offset_; }

  virtual off_t FindNextCutMark() const = 0;

 private:
  template <class ChunkGeneratorT>
  static void RegisterChunkGenerator() {
    registered_chunk_generators_.push_back(
      new ChunkGeneratorFactoryImpl<ChunkGeneratorT>());
  }
  static void RegisterChunkGenerators();

 protected:
  static const size_t               minimal_chunk_size_;
  static const size_t               maximal_chunk_size_;
  static const size_t               average_chunk_size_;

 private:
  typedef std::vector<ChunkGeneratorFactory*> RegisteredChunkGenerators;
  static RegisteredChunkGenerators  registered_chunk_generators_;

  const MemoryMappedFile           &mmf_;
  off_t                             offset_;
};

template <class ChunkGeneratorT>
ChunkGenerator* ChunkGeneratorFactoryImpl<ChunkGeneratorT>::Construct(const MemoryMappedFile &mmf) const {
  ChunkGenerator* generator = new ChunkGeneratorT(mmf);
  return generator;
}

class NaiveChunkGenerator : public ChunkGenerator {
 public:
  NaiveChunkGenerator(const MemoryMappedFile &mmf) : ChunkGenerator(mmf) {};
  static bool  WillHandleFile(const MemoryMappedFile &mmf);

 protected:
  off_t        FindNextCutMark() const;
};

}

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_H_ */
