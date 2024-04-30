/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_FILE_H_
#define CVMFS_COMPRESSION_INPUT_FILE_H_

#include "input_abstract.h"

namespace zlib {

class InputFile : public InputAbstract {
 public:
  InputFile(const FILE* src, const size_t max_chunk_size,
            const bool is_owner = false);
  virtual ~InputFile();
  virtual bool NextChunk();
  virtual bool IsValid();

 private:
  const FILE *src_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_FILE_H_
