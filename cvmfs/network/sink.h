/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_H_
#define CVMFS_NETWORK_SINK_H_

#include <stdint.h>
#include <string>

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
  virtual ~Sink() { }
  /**
   * Appends data to the sink
   * 
   * @returns on success: number of bytes written 
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) = 0;

  /**
   * Truncate all written data and start over at position zero.
   * 
   * @returns Success = 0
   *          Failure = -1
   */
  virtual int Reset() = 0;

  /**
    * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid() = 0;

  /**
   * Release ownership of the sink's resource
   */
  void Release() { is_owner_ = false; }

  /**
   * Commit data to the sink
   * @returns success 0
   *          otherwise failure
   */
  virtual int Flush() = 0;

  /**
   * Allocate space in the sink
   * 
   * @returns success 0
   */
  virtual int Reserve(size_t size) = 0;

  /**
   * Returns if the specific sink type needs reservation of (data) space
   * 
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() = 0;

  /**
   * Return a string representation of the sink
  */
  virtual std::string ToString() = 0;

 protected:
    bool is_owner_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_H_
