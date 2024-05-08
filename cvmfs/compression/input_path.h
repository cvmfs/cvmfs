/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_INPUT_PATH_H_
#define CVMFS_COMPRESSION_INPUT_PATH_H_

#include <string>

#include "input_abstract.h"
#include "input_file.h"
#include "util/pointer.h"

namespace zlib {

class InputPath : public InputAbstract {
 public:
  explicit InputPath(const std::string& path);
  InputPath(const std::string& path, const size_t max_chunk_size);
  virtual ~InputPath() { }

  virtual size_t chunk_size() const { return input_file_->chunk_size(); }
  virtual unsigned char* chunk() const { return input_file_->chunk(); }
  virtual bool NextChunk() { return input_file_->NextChunk(); }
  virtual bool IsValid() { return input_file_->IsValid(); }
  virtual bool Reset() { return input_file_->Reset(); }
  virtual bool has_chunk_left() const { return input_file_->has_chunk_left(); }

  std::string path() const {return path_; }

 private:
  std::string path_;
  FILE *file_;
  UniquePtr<InputFile> input_file_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_INPUT_PATH_H_
