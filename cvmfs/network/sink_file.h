/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_FILE_H_
#define CVMFS_NETWORK_SINK_FILE_H_

#include <cerrno>
#include <cstdio>
#include <string>

#include "sink.h"
#include "util/posix.h"

namespace cvmfs {

class FileSink : public Sink {
 public:
  explicit FileSink(FILE *destination_file) : Sink(true),
                                              file_(destination_file) { }

  virtual ~FileSink() { }

  /**
   * Appends data to the sink
   *
   * @returns on success: number of bytes written (can be less than requested)
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

  /**
   * Purges all resources leaving the sink in an invalid state.
   * More aggressive version of Reset().
   * For some sinks it might do the same as Reset().
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
  virtual bool IsValid() {
    return file_ != NULL;
  }

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() {
    return fflush(file_) == 0 ? 0 : -errno;
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
  virtual bool Reserve(size_t /*size*/) {
    return true;
  }

  /**
   * Returns if the specific sink type needs reservation of (data) space
   *
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() {
    return false;
  }

  /**
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe();

  FILE* file() { return file_; }



 private:
  FILE *file_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_FILE_H_
