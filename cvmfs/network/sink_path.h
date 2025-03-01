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

namespace cvmfs {

/**
 * PathSink is a data sink that writes to a file given by a path.
 * 
 * Internally it uses a FileSink that has ownership of the file.
 * (Though as PathSink is owner of the FileSink, to the outside also PathSink
 *  is considered to be the owner.)
 * 
 * Like FileSink, PathSink does not require to reserve space.
 * Contrary to FileSink, PathSink does not allow to adopt and write to a
 * different file path.
 */
class PathSink : public Sink {
 public:
  explicit PathSink(const std::string &destination_path);

  virtual ~PathSink() { }  // UniquePtr<FileSink> sink_ takes care of everything

  /**
   * Appends data to the sink
   * 
   * @returns on success: number of bytes written (can be less than requested)
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
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe();

  const std::string path() { return path_; }

 private:
  FILE *file_;  // owned by sink_
  UniquePtr<FileSink> sink_;
  const std::string path_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_PATH_H_
