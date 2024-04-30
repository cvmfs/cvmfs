/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_ABSTRACT_H_
#define CVMFS_COMPRESSION_INPUT_ABSTRACT_H_

#include <cstdio>

namespace zlib {

class InputAbstract {
 protected:
  InputAbstract(const bool is_owner, const size_t max_chunk_size,
                unsigned char *chunk = NULL) :
                                                is_owner_(is_owner),
                                                has_chunk_left_(false),
                                                max_chunk_size_(max_chunk_size),
                                                chunk_(chunk),
                                                chunk_size_(0) { }

 public:
  virtual ~InputAbstract() { }
  virtual bool NextChunk() = 0;
  virtual bool IsValid() = 0;

  virtual bool has_chunk_left() const { return has_chunk_left_;  }
  virtual size_t max_chunk_size() const { return max_chunk_size_; }
  virtual size_t chunk_size() const { return chunk_size_; }
  virtual unsigned char* chunk() const { return chunk_; }

 protected:
  const bool is_owner_;
  bool has_chunk_left_;
  const size_t max_chunk_size_;
  unsigned char *chunk_;
  size_t chunk_size_;  // must be <= chunk_size_
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_ABSTRACT_H_
