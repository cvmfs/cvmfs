/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_FD_H_
#define CVMFS_COMPRESSION_INPUT_FD_H_

#include "input_abstract.h"
#include "compression.h"
#include "compression/util.h"

namespace zip {

/**
 * Read-only data source: allows chunked reading of a file given by a FILE*
 */
class InputFd : public InputAbstract {
 public:
  InputFd(const int fd, const size_t max_chunk_size = zip::kZChunk,
            const bool is_owner = false);
  virtual ~InputFd();

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
   * Resets the reading progress of a valid source. The next call to NextChunk()
   * will start reading from the beginning.
   */
  virtual bool Reset();

 private:
  const int src_;
  bool is_valid_;
};

}  // namespace zip

#endif  // CVMFS_COMPRESSION_INPUT_FD_H_
