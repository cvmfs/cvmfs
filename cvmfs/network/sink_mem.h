/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_MEM_H_
#define CVMFS_NETWORK_SINK_MEM_H_

#include <cassert>
#include <cstring>
#include <string>

#include "sink.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

namespace cvmfs {

class MemSink : public Sink {
 public:
  MemSink() : size_(0), pos_(0), data_(NULL), max_size_(4 * 1024 * 1024)
              { is_owner_ = true; }
  explicit MemSink(size_t size) : size_(size), pos_(0) {
    data_ = static_cast<unsigned char *>(smalloc(size));
    is_owner_ = true;
  }

  virtual ~MemSink() { FreeData(); }

  /**
   * Appends data to the sink
   * If the sink is too small and
   * - the sink is the owner of data_: sink size is doubled
   * - the sink is NOT the owner of data_: fails with -ENOSPC
   * 
   * @returns on success: number of bytes written 
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) {
    if (pos_ + sz > size_) {
      if (is_owner_) {
        size_t new_size = pos_ + sz < size_ * 2 ? size_ * 2 : pos_ + sz + 1;
        data_ = static_cast<unsigned char *>(srealloc(data_, new_size));
        size_ = new_size;
      } else {
        return -ENOSPC;
      }
    }

    memcpy(data_ + pos_, buf, sz);
    pos_ += sz;
    return sz;
  }

  /**
   * Truncate all written data and start over at position zero.
   * 
   * @returns Success = 0
   *          Failure = -1
   */
  virtual int Reset() {
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
  virtual bool IsValid() {
    return (size_ == 0 && pos_ == 0 && data_ == NULL) ||
           (size_ > 0 && pos_ >= 0 && data_ != NULL);
  }

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() {
    return 0;
  }

  /**
   * Allocate space in the sink.
   * Always returns true if the specific sink does not need this.
   * 
   * For this memSink the maximum supported buffer size is 4 MiB.
   * Any request to reserve a larger chunk will result in failure
   * 
   * @returns success = true
   *          failure = false
   */
  virtual bool Reserve(size_t size) {
    if (size > max_size_) {
      return false;
    }

    FreeData();

    is_owner_ = true;
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
   * Returns if the specific sink type needs reservation of (data) space
   * 
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() {
    return true;
  }

  /**
   * Return a string representation of the sink
  */
  virtual std::string Describe() {
    std::string result = "Memory sink with ";
    result += "size: " + StringifyUint(size_);
    result += " - current pos: " + StringifyUint(pos_);
    return result;
  }


  void Adopt(size_t size, size_t pos, unsigned char *data, bool is_owner = true) {
    assert(size >= pos);

    FreeData();

    is_owner_ = is_owner;
    size_ = size;
    pos_ = pos;
    data_ = data;
  }

 public:
  size_t size_;
  size_t pos_;
  unsigned char *data_;

 private:
  void FreeData() {
    if (is_owner_) {
      free(data_);
    }
  }

  size_t max_size_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_MEM_H_
