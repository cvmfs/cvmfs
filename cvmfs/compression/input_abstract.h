/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_ABSTRACT_H_
#define CVMFS_COMPRESSION_INPUT_ABSTRACT_H_

#include <cstdio>

namespace zlib {

/**
 * An abstract read-only data source that allows to read a source in chunks.
 *
 * Usage:
 * After calling the constructor, the very next step is to call NextChunk().
 * When NextChunk() returns true all necessary processing was successful, that
 * chunk() and chunk_size() are correctly set. It also updates has_chunk_left().
 * has_chunk_left() can be useful in cases where the last chunk needs special
 * processing.
 *
 * @note
 * A single, zero-length read of empty sources are also supported. In that case
 * chunk_size() = 0 and chunk() can be NULL.
 *
 */
class InputAbstract {
 protected:
  InputAbstract(const bool is_owner, const size_t max_chunk_size,
                unsigned char *chunk = NULL) :
                                                is_owner_(is_owner),
                                                has_chunk_left_(false),
                                                max_chunk_size_(max_chunk_size),
                                                chunk_(chunk),
                                                chunk_size_(0),
                                                idx_inside_chunk_(0),
                                                bytes_read_(0) { }

 public:
  virtual ~InputAbstract() { }
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
  virtual bool NextChunk() = 0;
  /**
   * Data source is a valid source
   */
  virtual bool IsValid() = 0;
  /**
   * Resets the reading progress of a valid source. The next call to NextChunk()
   * will start reading from the beginning.
   */
  virtual bool Reset() = 0;

  /**
   * Returns true if there are more chunks to read and another NextChunk()-call
   * should be performed.
   */
  virtual bool has_chunk_left() const { return has_chunk_left_;  }
  /**
   * Maximum size of a chunk
   */
  virtual size_t max_chunk_size() const { return max_chunk_size_; }
  /**
   * Size of the current chunk.
   * Can be anywhere in size from: 0 <= chunk_size <= max_chunk_size
   */
  virtual size_t chunk_size() const { return chunk_size_; }
  /**
   * Current chunk of a read-only data source.
   *
   * @note Do NOT modify the content to where chunk() points if you are not the
   *       derived class
   */
  virtual unsigned char* chunk() const { return chunk_; }

  /**
   * Total bytes read so far (including current chunk).
   * Includes the current chunk that might not have been processed yet by the
   * user.
   */
  virtual size_t bytes_read() const { return bytes_read_; }

  // TODO TODO check if condition is correct or off-by-one error
  virtual bool HasInputLeftInChunk() const
             { return idx_inside_chunk_ < chunk_size_ - 1 && chunk_size_ != 0; }
  virtual void SetIdxInsideChunk(const size_t idx) { idx_inside_chunk_ = idx; }
  virtual size_t GetIdxInsideChunk() const { return idx_inside_chunk_; }

 protected:
  const bool is_owner_;  // Input class is owner of the data source
  bool has_chunk_left_;
  const size_t max_chunk_size_;
  unsigned char *chunk_;
  size_t chunk_size_;  // must be <= max_chunk_size_
  size_t idx_inside_chunk_;
  size_t bytes_read_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_ABSTRACT_H_
