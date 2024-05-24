/**
 * This file is part of the CernVM File System.
 */

#include "input_cache.h"

#include <cstdlib>

#include "util/smalloc.h"

namespace zlib {

InputCache::InputCache(CacheManager *mgr, const int fd, const size_t buf_size) :
                                              InputAbstract(true, buf_size),
                                              mgr_(mgr), fd_(fd) {
  if (InputCache::IsValid()) {
    has_chunk_left_ = true;
    idx_ = 0;
    chunk_ = static_cast<unsigned char*>(smalloc(max_chunk_size_));
  } else {
    idx_ = -1;
  }
}

InputCache::~InputCache() {
  if (InputCache::IsValid()) {
    free(chunk_);
  }
}

bool InputCache::NextChunk() {
  if (!has_chunk_left_) {
    return false;
  }

  chunk_size_ = mgr_->Pread(fd_, chunk_, max_chunk_size_, idx_);

  idx_ += chunk_size_;

  if (chunk_size_ < max_chunk_size_) {
    has_chunk_left_ = false;
  }

  return true;
}

bool InputCache::IsValid() {
  return fd_ >= 0 && mgr_ != NULL;
}

bool InputCache::Reset() {
  if (IsValid()) {
    has_chunk_left_ = true;
    idx_ = 0;
    chunk_size_ = 0;
    return true;
  }
  return false;
}

}  // namespace zlib
