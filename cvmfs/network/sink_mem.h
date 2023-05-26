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
  MemSink() : Sink(true), size_(0), pos_(0),
              data_(NULL), max_size_(kMaxMemSize) { }
  explicit MemSink(size_t size);
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
  virtual int64_t Write(const void *buf, uint64_t sz);

  /**
   * Truncate all written data and start over at position zero.
   *
   * @returns Success = 0
   *          Failure = -1
   */
  virtual int Reset();

  virtual int Purge() {
    return Reset();
  }

  /**
    * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid();

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() {
    return 0;
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
  virtual bool Reserve(size_t size);

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
  virtual std::string Describe();


  void Adopt(size_t size, size_t pos, unsigned char *data,
             bool is_owner = true);

  size_t size() { return size_; }
  size_t pos() { return pos_; }
  unsigned char* data() { return data_; }

  /**
   * Do not download files larger than 1M into memory.
   */
  static const size_t kMaxMemSize = 1024*1024;

 private:
  void FreeData() {
    if (is_owner_) {
      free(data_);
    }
  }

  size_t size_;
  size_t pos_;
  unsigned char *data_;
  const size_t max_size_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_MEM_H_
