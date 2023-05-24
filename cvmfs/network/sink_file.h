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
  explicit FileSink(FILE *destination_file) : file_(destination_file) {
    is_owner_ = false;
    assert(file_ != NULL);
  }

  virtual ~FileSink() { }

  /**
   * Appends data to the sink
   * 
   * @returns on success: number of bytes written 
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) {
    fwrite(buf, 1ul, sz, file_);

    if (ferror(file_) != 0) {
      // ferror does not tell us what exactly the error is
      // and errno is also not set
      // so just return generic I/O error flag
      return -EIO;
    }

    return sz;
  }

  /**
   * Truncate all written data and start over at position zero.
   * 
   * @returns Success = 0
   *          Failure = -1
   */
  virtual int Reset() {
    return ((fflush(file_) == 0) &&
           (ftruncate(fileno(file_), 0) == 0) &&
           (freopen(NULL, "w", file_) == file_)) ? 0 : -errno;
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
  virtual std::string Describe() {
    std::string result = "File sink with ";
    result += IsValid() ? " valid file pointer" : " invalid file pointer";
    return result;
  }



 public:
  FILE *file_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_FILE_H_
