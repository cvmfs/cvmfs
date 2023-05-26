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
   * Allocate space in the sink.
   * Always returns true if the specific sink does not need this.
   * 
   * @returns success = true
   *          failure = false
   */
  virtual bool Reserve(size_t size) {
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
   * Return a string representation of the sink
  */
  virtual std::string Describe();

  FILE* file() { return file_; }



 private:
  FILE *file_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_FILE_H_
