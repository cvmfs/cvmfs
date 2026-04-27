/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_MEM_H_
#define CVMFS_COMPRESSION_INPUT_MEM_H_

#include "input_abstract.h"

namespace zip {

/**
 * Read-only data source: allows chunked reading of a memory buffer
 * 
 * InputMem(NULL, 0) is allowed and will have 1 successful call to NextChunk().
 * Reset() will not work on such a NULL object.
 */
class InputMem : public InputAbstract {
 public:
  InputMem(const unsigned char *src, const size_t src_size);
  InputMem(const unsigned char *src, const size_t src_size,
           const size_t max_chunk_size, bool is_owner = false);
  virtual ~InputMem();
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
   * Data source is a valid source
   */
  virtual bool IsValid();
  /**
   * Resets the reading progress of the source, the next call to NextChunk()
   * will start reading from the beginning.
   */
  virtual bool Reset();

 private:
  const unsigned char *src_;
  const size_t src_size_;
  size_t idx_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_MEM_H_
