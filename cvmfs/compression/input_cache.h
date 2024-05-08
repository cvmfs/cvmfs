/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_CACHE_H_
#define CVMFS_COMPRESSION_INPUT_CACHE_H_

#include "input_abstract.h"

#include <stdint.h>

#include "cache.h"


// class CacheManager;
// int64_t CacheManager::Pread(int /*fd*/, void * /*buf*/,
//                             uint64_t /*size*/, uint64_t /*offset*/);

// typedef int64_t (ChacheManager::*PreadFunc) (int /*fd*/, void * /* buf*/,
//                                      uint64_t /*size*/, uint64_t /*offset*/);

namespace zlib {

class InputCache : public InputAbstract {
 public:
  InputCache(CacheManager *mgr, const int fd, const size_t buf_size);
  virtual ~InputCache();
  virtual bool NextChunk();
  virtual bool IsValid();
  virtual bool Reset();

 private:
  CacheManager *mgr_;
  const int fd_;
  uint64_t idx_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_CACHE_H_
