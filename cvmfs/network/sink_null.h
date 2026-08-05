/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_NULL_H_
#define CVMFS_NETWORK_SINK_NULL_H_

#include <cassert>
#include <cstring>
#include <string>

#include "sink.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

namespace cvmfs {

/**
 * NullSink is a "/dev/null"-like data sink that only counts how many bytes
 * would have been written but DOES NOT SAVE ANY OF THE DATA given to Write().
 */
// try https://stackoverflow.com/questions/3065154/undefined-reference-to-vtable
class NullSink : public Sink {
 public:
  NullSink() : Sink(true), dropped_bytes_(0) { }
  virtual ~NullSink() { }

  /**
   * Appends data to the sink
   * If the sink is too small and
   * - the sink is the owner of data_: sink size is increased
   * - the sink is NOT the owner of data_: fails with -ENOSPC
   *
   * @returns on success: number of bytes written (can be less than requested)
   *          on failure: -errno.
   */
  virtual int64_t Write(const void */*buf*/, uint64_t sz)
                      { dropped_bytes_ += sz; return static_cast<int64_t>(sz); }

  /**
   * Truncate all written data and start over at position zero.
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Reset() { dropped_bytes_ = 0; return 0; }

  /**
   * Purges all resources leaving the sink in an invalid state.
   * More aggressive version of Reset().
   * For some sinks and depending on owner status it might do
   * the same as Reset().
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Purge() { return Reset(); }

  /**
    * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid() { return true; }

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() { return 0; }

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
  virtual bool Reserve(size_t /*size*/) { Reset(); return true; }

  /**
   * Returns if the specific sink type needs reservation of (data) space
   *
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() { return false; }

  /**
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe()
             { return "Null sink with size: " + StringifyUint(dropped_bytes_); }

  size_t dropped_bytes() { return dropped_bytes_; }

 private:
  size_t dropped_bytes_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_NULL_H_
