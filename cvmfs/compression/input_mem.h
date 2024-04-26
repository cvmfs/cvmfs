/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_MEM_H_
#define CVMFS_COMPRESSION_INPUT_MEM_H_

#include "input_abstract.h"

namespace zlib {

class InputMem : public InputAbstract {
 public:
  InputMem(const unsigned char *src, const size_t src_size,
           const size_t max_chunk_size, bool is_owner=false);
  virtual ~InputMem();
  virtual bool NextChunk();
  virtual bool IsValid();

 private:
  const unsigned char *src_;
  const size_t src_size_;
  size_t idx_;
};

} // namespace zlib

#endif // CVMFS_COMPRESSION_INPUT_MEM_H_