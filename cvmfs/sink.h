/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SINK_H_
#define CVMFS_SINK_H_

#include <stdint.h>

namespace cvmfs {

/**
 * A data sink that behaves like a writable file descriptor with a custom
 * implementation.
 *
 * Currently used by the Fetcher class to redirect writing into a cache manager.
 *
 * TODO(jblomer): can all download destinations be implemented by inheriting
 * from this class?
 */
class Sink {
 public:
  Sink(void *user_data) : user_data_(user_data) { };
  virtual ~Sink() { }
  /**
   * Appends data to the sink, returns the number of bytes written or -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) = 0;
  /**
   * Truncate all written data and start over at position zero.
   */
  virtual int Reset() = 0;

 protected:
  void *user_data_;
};

}  // namespace cvmfs

#endif  // CVMFS_SINK_H_
