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

/**
 * FileSink is a data sink that write to a given FILE.
 * It does not require any space reservation. It can change to which file to
 * write to using Adopt().
 *
 * By default FileSink is not the owner of FILE, so the outside entity has to
 * take care of opening and closing the FILE.
 */
class FileSink : public Sink {
 public:
  explicit FileSink(FILE *destination_file)
      : Sink(false), file_(destination_file) { }
  FileSink(FILE *destination_file, bool is_owner)
      : Sink(is_owner), file_(destination_file) { }

  virtual ~FileSink() {
    if (is_owner_ && file_) {
      (void)fclose(file_);
    }
  }

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

  virtual int Purge();

  /**
   * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid() { return file_ != NULL; }

  /**
   * Commit data to the sink
   * @returns success = 0
   *          failure = -errno
   */
  virtual int Flush() { return fflush(file_) == 0 ? 0 : -errno; }

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
  virtual bool Reserve(size_t /*size*/) { return true; }

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
  virtual std::string Describe();

  /**
   * Allows the sink to adopt data that was initialized outside this class.
   * The sink can become the new owner of the data, or not.
   */
  void Adopt(FILE *file, bool is_owner = true);

  FILE *file() { return file_; }


 private:
  FILE *file_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_FILE_H_
