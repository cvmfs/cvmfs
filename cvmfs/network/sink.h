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
 */
class Sink {
 protected:
  explicit Sink(bool is_owner) : is_owner_(is_owner) { }

 public:
  virtual ~Sink() { }
  /**
   * Appends data to the sink
   *
   * @returns on success: number of bytes written (can be less than requested)
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) = 0;

  /**
   * Truncate all written data and start over at position zero.
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Reset() = 0;

  /**
   * Purges all resources leaving the sink in an invalid state.
   * More aggressive version of Reset().
   * For some sinks and depending on owner status it might do
   * the same as Reset().
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Purge() = 0;

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
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Flush() = 0;

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
  virtual bool Reserve(size_t size) = 0;

  /**
   * Returns if the specific sink type needs reservation of (data) space
   *
   * @returns true  - reservation is needed
   *          false - no reservation is needed
   */
  virtual bool RequiresReserve() = 0;

  /**
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe() = 0;

 protected:
  bool is_owner_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_H_
