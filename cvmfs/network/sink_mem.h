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

namespace cvmfs {

/**
 * MemSink is a data sink that writes to a unsigned char* buffer.
 * The buffer has a fixed size, as such reservation of space is necessary.
 * It can use Adopt() to write to a different buffer.
 *
 * By default, MemSink is the owner of the buffer and takes care of its
 * creation and deletion.
 */
class MemSink : public Sink {
 public:
  MemSink() : Sink(true), size_(0), pos_(0),
              data_(NULL), max_size_(kMaxMemSize) { }
  explicit MemSink(size_t size);
  virtual ~MemSink() { FreeData(); }

  /**
   * Appends data to the sink
   * If the sink is too small and
   * - the sink is the owner of data_: sink size is increased
   * - the sink is NOT the owner of data_: fails with -ENOSPC
   *
   * @returns on success: number of bytes written (can be less than requested)
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz);

  /**
   * Truncate all written data and start over at position zero.
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Reset();

  /**
   * Purges all resources leaving the sink in an invalid state.
   * More aggressive version of Reset().
   * For some sinks and depending on owner status it might do
   * the same as Reset().
   *
   * @returns Success = 0
   *          Failure = -errno
   */
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
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe();

  /**
   * Allows the sink to adopt data that was initialized outside this class.
   * The sink can become the new owner of the data, or not.
   */
  void Adopt(size_t size, size_t pos, unsigned char *data,
             bool is_owner = true);

  size_t size() { return size_; }
  size_t pos() { return pos_; }
  unsigned char* data() { return data_; }

  /**
   * Do not download files larger than 1M into memory.
   */
  static const size_t kMaxMemSize = 1024ul * 1024ul;

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
