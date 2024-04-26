/**
 * This file is part of the CernVM File System.
 */

#include "input_file.h"

namespace zlib {

InputFile::InputFile(const FILE* src, const size_t max_chunk_size,
                     const bool is_owner) :
                                  InputAbstract(is_owner, max_chunk_size, NULL),
                                  src_(src) {
  if (IsValid()) {
    chunk_ = static_cast<char*>(smalloc(max_chunk_size_));
    has_chunk_left_ = true;
  }
}

InputFile::~InputFile() {
  if (IsValid()) {
    free(chunk_);

    if (is_owner_) {
      close(src);
    }
  }
}



bool InputFile::NextChunk() {
  chunk_size_ = fread(chunk_, 1, max_chunk_size_, src_);

  if (ferror(src_)) {
    return false;
  }

  if (chunk_size_ < max_chunk_size) {
    if (feof(src_)) {
      has_chunk_left_ = false;
    }
  }

  return true;
}

bool InputFile::IsValid() {
  return src_ != NULL;
}



} // namespace zlib
