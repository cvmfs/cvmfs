/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_CACHE_H_
#define CVMFS_COMPRESSION_INPUT_CACHE_H_

#include "input_abstract.h"

#include <stdint.h>

#include "cache.h"

namespace zip {

/**
 * Read-only data source: allows chunked reading of a file given by a file
 *                        descriptor that is handled by a cache manager
 * 
 * Uses the Pread() function of the CacheManager to travers the file.
 * 
 * int64_t CacheManager::Pread(int fd, void *buf, uint64_t size,
 *                             uint64_t offset);
 */
class InputCache : public InputAbstract {
 public:
  InputCache(CacheManager *mgr, const int fd, const size_t buf_size);
  virtual ~InputCache();
  /**
   * Does all necessary processing to get the next chunk, so that chunk() and
   * chunk_size() are in valid states.
   *
   * @note empty data sources should also be correctly supported with returning
   *       for the very first call of NextChunk() true, setting chunk_size = 0,
   *       and it is ok if chunk() is returning NULL.
   *
   * @returns true on success
   *          false otherwise
   */
  virtual bool NextChunk();
  /**
   * Data source is a valid source: file descriptor is valid number
   */
  virtual bool IsValid();
  /**
   * Resets the reading progress of a valid source. The next call to NextChunk()
   * will start reading from the beginning.
   */
  virtual bool Reset();

 private:
  CacheManager *mgr_;
  const int fd_;
  uint64_t idx_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_CACHE_H_
