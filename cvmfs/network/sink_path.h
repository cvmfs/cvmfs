/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_PATH_H_
#define CVMFS_NETWORK_SINK_PATH_H_

#include <string>

#include "sink.h"
#include "util/posix.h"

namespace cvmfs {

class PathSink : public Sink {
 public:
  explicit PathSink(const std::string &destination_path) :
                                                       path_(destination_path) {
    is_owner_ = true;
    file_ = fopen(destination_path.c_str(), "w");
    assert(file_ != NULL);
  }

  virtual ~PathSink() { if (is_owner_ && file_) { fclose(file_); } }

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
           (freopen(NULL, "w", file_) == file_)) ? 0 : -1;
  }

  /**
   * @returns true if the object is correctly initialized.
   */
  bool IsValid() {
    return file_ != NULL;
  }

  /**
   * Commit data to the sink
   * @returns success 0
   *          otherwise failure
   */
  int Flush() {
    // A zero value indicates success.
    // For error: EOF is returned and the error indicator is set (see ferror)
    return fflush(file_) * -1;
  }

  /**
   * Allocate space in the sink
   * 
   * @returns success 0
   */
  int Reserve(size_t size) {
    return 0;
  }

  /**
   * Returns if the specific sink type needs reservation of (data) space
   * 
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  bool RequiresReserve() {
    return false;
  }

  /**
   * Return a string representation of the sink
  */
  std::string ToString() {
    std::string result = "Path sink for ";
    result += "path " + path_ + " and ";
    result += IsValid() ? " valid file pointer" : " invalid file pointer";
    return result;
  }

 public:
  FILE *file_;
  const std::string &path_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_PATH_H_
