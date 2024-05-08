/**
 * This file is part of the CernVM File System.
 */

#include "input_file.h"
#include "util/smalloc.h"

namespace zlib {

InputFile::InputFile(const FILE* src, const size_t max_chunk_size,
                     const bool is_owner) :
                                  InputAbstract(is_owner, max_chunk_size, NULL),
                                  src_(src) {
  if (IsValid()) {
    chunk_ = static_cast<unsigned char*>(smalloc(max_chunk_size_));
    has_chunk_left_ = true;
  }
}

InputFile::~InputFile() {
  if (IsValid()) {
    free(chunk_);

    if (is_owner_) {
      fclose(const_cast<FILE*>(src_));
    }
  }
}



bool InputFile::NextChunk() {
  chunk_size_ = fread(chunk_, 1, max_chunk_size_, const_cast<FILE*>(src_));

  if (ferror(const_cast<FILE*>(src_))) {
    has_chunk_left_ = false;

    return false;
  }

  if (chunk_size_ < max_chunk_size_) {
    if (feof(const_cast<FILE*>(src_))) {
      has_chunk_left_ = false;
    }
  }

  return true;
}

bool InputFile::IsValid() {
  return src_ != NULL;
}

bool InputFile::Reset() {
  if (IsValid()) {
    fseek(const_cast<FILE*>(src_), 0L, SEEK_SET);
    chunk_size_ = 0;
    has_chunk_left_ = true;
    return true;
  }
  return false;
}

}  // namespace zlib
