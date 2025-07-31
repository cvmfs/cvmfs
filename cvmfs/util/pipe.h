/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_PIPE_H_
#define CVMFS_UTIL_PIPE_H_

#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>

#include "exception.h"
#include "duplex_testing.h"
#include "util/export.h"
#include "util/logging.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Describes the functionality of a pipe used as a template parameter to
 * the Pipe class. This makes it clear in stack traces which pipe is blocking.
 */
enum PipeType {
  kPipeThreadTerminator = 0,  // pipe only used to signal a thread to stop
  kPipeWatchdog,
  kPipeWatchdogSupervisor,
  kPipeWatchdogPid,
  kPipeDetachedChild,
  kPipeTest,
  kPipeDownloadJobs,
  kPipeDownloadJobsResults
};

/**
 * Common signals used by pipes
 */
enum PipeSignals {
  kPipeTerminateSignal = 1
};

template<PipeType pipeType>
class CVMFS_EXPORT Pipe : public SingleCopy {
  FRIEND_TEST(T_Util, ManagedExecRunShell);
  FRIEND_TEST(T_Util, ManagedExecExecuteBinaryDoubleFork);
  FRIEND_TEST(T_Util, ManagedExecExecuteBinaryAsChild);

 public:
  /**
   * A pipe is a simple asynchronous communication mechanism. It establishes
   * a unidirectional communication link between two file descriptors. One of
   * them is used to only write to the pipe, the other one only to read from it.
   *
   * This class is a simple wrapper around the handling of a "standard" pipe
   * that uses system calls.
   *
   * @note PipeType as class template parameter should symbolize the
   *       functionality of the specific type, independent of what variable
   *       name it has
   */
  Pipe() {
    int pipe_fd[2];
    MakePipe(pipe_fd);
    fd_read_ = pipe_fd[0];
    fd_write_ = pipe_fd[1];
  }

  /**
   * Destructor closes all valid file descriptors of the pipe
   */
  ~Pipe() { Close(); }

  /**
   * Closes all open file descriptors of the pipe and marks them as invalid
   */
  void Close() {
    CloseReadFd();
    CloseWriteFd();
  }

  /**
   * Closes file descriptor that reads from the pipe and marks it as invalid.
   */
  void CloseReadFd() {
    if (fd_read_ >= 0) {
      close(fd_read_);
      fd_read_ = -1;
    }
  }

  /**
   * Closes file descriptor that writes to the pipe and marks it as invalid.
   */
  void CloseWriteFd() {
    if (fd_write_ >= 0) {
      close(fd_write_);
      fd_write_ = -1;
    }
  }

  /**
   * Tries to write an object to the pipe
   *
   * @returns true if the entire object was written
   *          false otherwise
   */
  template<typename T>
  bool TryWrite(const T &data) {
    const int num_bytes = write(fd_write_, &data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  /**
   * Writes an object to the pipe
   *
   * @returns true on success
   *          otherwise kills the program with an assert
   *
   */
  template<typename T>
  bool Write(const T &data) {
    WritePipe(fd_write_, &data, sizeof(T));
    return true;
  }

  /**
   * Writes an object to the pipe
   * If possible, it is recommended to use "bool Write(const T &data)"
   *
   * @returns true on success
   *          otherwise kills the program with an assert
   *
   */
  bool Write(const void *buf, size_t nbyte) {
    WritePipe(fd_write_, buf, nbyte);
    return true;
  }

  /**
   * (Re)tries to read from the pipe until it receives data or returned error
   * is NOT a system interrupt
   *
   * @returns true if sizeof(data) bytes were received
   *          false otherwise
   */
  template<typename T>
  bool TryRead(T *data) {
    ssize_t num_bytes;
    do {
      num_bytes = read(fd_read_, data, sizeof(T));
    } while ((num_bytes < 0) && (errno == EINTR));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  /**
   * Reads an object from the pipe
   *
   * @returns true on success
   *          otherwise kills the program with an assert
   */
  template<typename T>
  bool Read(T *data) {
    ReadPipe(fd_read_, data, sizeof(T));
    return true;
  }

  /**
   * Reads an object from the pipe
   * If possible, it is recommend to use "bool Read(T *data)""
   *
   * @returns true on success
   *          otherwise kills the program with an assert
   */
  bool Read(void *buf, size_t nbyte) {
    ReadPipe(fd_read_, buf, nbyte);
    return true;
  }

  /**
   * Returns the file descriptor that reads from the pipe
   */
  int GetReadFd() const { return fd_read_; }

  /**
   * Returns the file descriptor that writes to the pipe
   */
  int GetWriteFd() const { return fd_write_; }


 private:
  int fd_read_;
  int fd_write_;

  /**
   * Only used in the unit tests to test pipes using stdin/stdout as read/write.
   */
  Pipe(const int fd_read, const int fd_write)
      : fd_read_(fd_read), fd_write_(fd_write) { }

  /**
   * Creating a pipe should always succeed.
   */
  void MakePipe(int pipe_fd[2]) {
    const int retval = pipe(pipe_fd);
    if (retval != 0) {
      PANIC(kLogSyslogErr | kLogDebug, "MakePipe failed with errno %d", errno);
    }
  }


  /**
   * Writes to a pipe should always succeed.
   */
  void WritePipe(int fd, const void *buf, size_t nbyte) {
    ssize_t num_bytes;
    do {
      num_bytes = write(fd, buf, nbyte);
    } while ((num_bytes < 0) && (errno == EINTR));
    if (!((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte))) {
      PANIC(kLogSyslogErr | kLogDebug,
            "WritePipe failed: expected write size %lu, "
            "actually written %lu, errno %d, fd %d",
            nbyte, num_bytes, errno, fd);
    }
  }


  /**
   * Reads from a pipe should always succeed.
   */
  void ReadPipe(int fd, void *buf, size_t nbyte) {
    ssize_t num_bytes;
    do {
      num_bytes = read(fd, buf, nbyte);
    } while ((num_bytes < 0) && (errno == EINTR));
    if (!((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte))) {
      PANIC(kLogSyslogErr | kLogDebug,
            "ReadPipe failed: expected read size %lu, "
            "actually read %lu, errno %d, fd %d",
            nbyte, num_bytes, errno, fd);
    }
  }
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_PIPE_H_
