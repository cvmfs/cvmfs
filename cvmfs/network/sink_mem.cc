/**
 * This file is part of the CernVM File System.
 */

#include "sink_mem.h"

#include <cassert>
#include <cstring>
#include <string>

#include "util/smalloc.h"
#include "util/string.h"

namespace cvmfs {

MemSink::MemSink(size_t size)
    : Sink(true), size_(size), pos_(0), max_size_(kMaxMemSize) {
  data_ = static_cast<unsigned char *>(smalloc(size));
}

/**
 * Appends data to the sink
 * If the sink is too small and
 * - the sink is the owner of data_: sink size is doubled
 * - the sink is NOT the owner of data_: fails with -ENOSPC
 *
 * @returns on success: number of bytes written
 *          on failure: -errno.
 */
int64_t MemSink::Write(const void *buf, uint64_t sz) {
  if (pos_ + sz > size_) {
    if (is_owner_) {
      const size_t new_size = pos_ + sz < size_ * 2 ? size_ * 2 : pos_ + sz + 1;
      data_ = static_cast<unsigned char *>(srealloc(data_, new_size));
      size_ = new_size;
    } else {
      return -ENOSPC;
    }
  }

  memcpy(data_ + pos_, buf, sz);
  pos_ += sz;
  return static_cast<int64_t>(sz);
}

/**
 * Truncate all written data and start over at position zero.
 *
 * @returns Success = 0
 *          Failure = -errno
 */
int MemSink::Reset() {
  if (is_owner_) {
    free(data_);
    data_ = NULL;
    size_ = 0;
  }

  pos_ = 0;

  return 0;
}

/**
 * @returns true if the object is correctly initialized.
 */
bool MemSink::IsValid() {
  return (size_ == 0 && pos_ == 0 && data_ == NULL)
         || (size_ > 0 && pos_ >= 0 && data_ != NULL);
}

/**
 * Reserves new space in sinks that require reservation (see RequiresReserve)
 *
 * Successful if the requested size is smaller than already space reserved, or
 * if the sink is the owner of the data and can allocate enough new space.
 *
 * @note If successful, always resets the current position to 0.
 *
 * Fails if
 *     1) sink is not the owner of the data and more than the current size is
 *        requested
 *     2) more space is requested than allowed (max_size_)
 *
 * @returns success = true
 *          failure = false
 */
bool MemSink::Reserve(size_t size) {
  if (size <= size_) {
    pos_ = 0;
    return true;
  }
  if (!is_owner_ || size > max_size_) {
    return false;
  }

  FreeData();

  size_ = size;
  pos_ = 0;
  if (size == 0) {
    data_ = NULL;
  } else {
    data_ = static_cast<unsigned char *>(smalloc(size));
  }
  return true;
}

/**
 * Return a string representation describing the type of sink and its status
 */
std::string MemSink::Describe() {
  std::string result = "Memory sink with ";
  result += "size: " + StringifyUint(size_);
  result += " - current pos: " + StringifyUint(pos_);
  return result;
}

/**
 * Allows the sink to adopt data that was initialized outside this class.
 * The sink can become the new owner of the data, or not.
 */
void MemSink::Adopt(size_t size, size_t pos, unsigned char *data,
                    bool is_owner) {
  assert(size >= pos);

  FreeData();

  is_owner_ = is_owner;
  size_ = size;
  pos_ = pos;
  data_ = data;
}

}  // namespace cvmfs
