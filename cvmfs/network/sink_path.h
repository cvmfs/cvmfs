/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_PATH_H_
#define CVMFS_NETWORK_SINK_PATH_H_

#include <cerrno>
#include <cstdio>
#include <string>

#include "sink.h"
#include "sink_file.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace cvmfs {

class PathSink : public Sink {
 public:
  explicit PathSink(const std::string &destination_path);

  virtual ~PathSink() { if (is_owner_ && file_) { fclose(file_); } }

  /**
   * Appends data to the sink
   * 
   * @returns on success: number of bytes written 
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) {
    return sink_->Write(buf, sz);
  }

  /**
   * Truncate all written data and start over at position zero.
   * 
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Reset() {
    return sink_->Reset();
  }

  virtual int Purge();

  /**
   * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid() {
    return sink_->IsValid();
  }

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() {
    return sink_->Flush();
  }

  /**
   * Allocate space in the sink.
   * Always returns true if the specific sink does not need this.
   * 
   * @returns success = true
   *          failure = false
   */
  virtual bool Reserve(size_t size) {
    return sink_->Reserve(size);
  }

  /**
   * Returns if the specific sink type needs reservation of (data) space
   * 
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() {
    return sink_->RequiresReserve();
  }

  /**
   * Return a string representation of the sink
  */
  virtual std::string Describe();

  const std::string path() { return path_; }

 private:
  FILE *file_;
  UniquePtr<FileSink> sink_;
  const std::string path_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_PATH_H_
