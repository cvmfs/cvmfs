/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_MEM_H_
#define CVMFS_NETWORK_SINK_MEM_H_

#include <cassert>
#include <cstring>

#include "sink.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

namespace cvmfs {

class MemSink : public Sink {
 public:
  MemSink() : size_(0), pos_(0), data_(NULL) { is_owner_ = true; }
  explicit MemSink(size_t size) : size_(size), pos_(0) {
    data_ = static_cast<char *>(smalloc(size));
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
        data_ = static_cast<char *>(srealloc(data_, new_size));
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
    int ret = 0;
    if (is_owner_ && data_) {
      free(data_);
      ret = 0;
    }

    data_ = NULL;
    size_ = 0;
    pos_ = 0;

    return ret;
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
   * @returns success 0
   *          otherwise failure
   */
  virtual int Flush() {
    return 0;
  }

  /**
   * Allocate space in the sink
   * 
   * @returns success 0
   */
  virtual int Reserve(size_t size) {
    FreeData();

    is_owner_ = true;
    size_ = size;
    pos_ = 0;
    if (size == 0) {
      data_ = NULL;
      return 0;
    } else {
      data_ = static_cast<char *>(smalloc(size));
      return 0;
    }
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
  std::string ToString() {
    std::string result = "Memory sink with ";
    result += "size: " + StringifyUint(size_);
    result += " - current pos: " + StringifyUint(pos_);
    return result;
  }


  void Adopt(size_t size, size_t pos, char *data, bool is_owner = true) {
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
  char *data_;

 private:
  void FreeData() {
    if (is_owner_ && data_) {
      free(data_);
    }
  }
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_MEM_H_
