/**
 * This file is part of the CernVM File System.
 */

#include <unistd.h>
#include <errno.h>

#include "input_fd.h"
#include "util/smalloc.h"

namespace zip {

InputFd::InputFd(const int fd, const size_t max_chunk_size,
                     const bool is_owner) :
                                  InputAbstract(is_owner, max_chunk_size, NULL),
                                  src_(fd), is_valid_(true) {
  if (InputFd::IsValid()) {
    chunk_ = static_cast<unsigned char*>(smalloc(max_chunk_size_));
    has_chunk_left_ = true;
  }
}

InputFd::~InputFd() {
  if (InputFd::IsValid()) {
    free(chunk_);

    if (is_owner_) {
      close(src_);
    }
    is_valid_ = false;
  }
}



bool InputFd::NextChunk() {
  do {
    ssize_t read_ret;
    read_ret = read(src_, chunk_, max_chunk_size_);
    if (read_ret == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        return false;
      }
    }
    chunk_size_ = read_ret;
    if (chunk_size_ == 0) {
      has_chunk_left_ = false;
    }
    // short reads are normal, especially for FIFOs etc
    break;
  } while (true);

  bytes_read_ += chunk_size_;
  idx_inside_chunk_ = 0;

  return true;
}

bool InputFd::IsValid() {
  return is_valid_;
}

bool InputFd::Reset() {
  if (IsValid()) {
    chunk_size_ = 0;
    has_chunk_left_ = true;
    bytes_read_ = 0;
    return true;
  }
  return false;
}

}  // namespace zip
