/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FD_TABLE_H_
#define CVMFS_FD_TABLE_H_

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>

#include <cassert>
#include <vector>

#include "util/single_copy.h"

/**
 * Maintains integers mapped to custom open file descriptors.  File descriptors
 * can be added, removed, and accessed.  All operations take constant time.  The
 * maximum file descriptor number needs to be known upfront.
 *
 * Note that new file descriptors do not necessarily have the smallest available
 * number but any number between 0..max_open_fds.
 *
 * This class is used by a couple of cache managers.
 */
template<class HandleT>
class FdTable : SingleCopy {
 public:
  FdTable(unsigned max_open_fds, const HandleT &invalid_handle)
      : invalid_handle_(invalid_handle)
      , fd_pivot_(0)
      , fd_index_(max_open_fds)
      , open_fds_(max_open_fds, FdWrapper(invalid_handle_, 0)) {
    assert(max_open_fds > 0);
    for (unsigned i = 0; i < max_open_fds; ++i) {
      fd_index_[i] = i;
      open_fds_[i].index = i;
    }
  }

  /**
   * Used to restore the state.
   */
  void AssignFrom(const FdTable<HandleT> &other) {
    invalid_handle_ = other.invalid_handle_;
    fd_pivot_ = other.fd_pivot_;
    fd_index_.resize(other.fd_index_.size());
    open_fds_.resize(other.open_fds_.size(), FdWrapper(invalid_handle_, 0));
    for (unsigned i = 0; i < fd_index_.size(); ++i) {
      fd_index_[i] = other.fd_index_[i];
      open_fds_[i] = other.open_fds_[i];
    }
  }

  /**
   * Used to save the state.
   */
  FdTable<HandleT> *Clone() {
    FdTable<HandleT> *result = new FdTable<HandleT>(open_fds_.size(),
                                                    invalid_handle_);
    result->fd_pivot_ = fd_pivot_;
    for (unsigned i = 0; i < fd_index_.size(); ++i) {
      result->fd_index_[i] = fd_index_[i];
      result->open_fds_[i] = open_fds_[i];
    }
    return result;
  }


  /**
   * Registers fd with a currently unused number.  If the table is full,
   * returns -ENFILE;
   */
  int OpenFd(const HandleT &handle) {
    if (handle == invalid_handle_)
      return -EINVAL;
    if (fd_pivot_ >= fd_index_.size())
      return -ENFILE;

    size_t next_fd = fd_index_[fd_pivot_];
    assert(next_fd < open_fds_.size());
    assert(open_fds_[next_fd].handle == invalid_handle_);
    open_fds_[next_fd] = FdWrapper(handle, fd_pivot_);
    ++fd_pivot_;
    return next_fd;
  }

  /**
   * For invalid and unused numbers, the invalid handle is returned.
   */
  HandleT GetHandle(int fd) {
    return IsValid(fd) ? open_fds_[fd].handle : invalid_handle_;
  }


  /**
   * Releases fd back to the set of available numbers.  Gracefully handles
   * invalid handles (-EBADFD)
   */
  int CloseFd(int fd) {
    if (!IsValid(fd))
      return -EBADF;

    unsigned index = open_fds_[fd].index;
    assert(index < fd_index_.size());
    assert(fd_pivot_ <= fd_index_.size());
    assert(fd_pivot_ > 0);
    open_fds_[fd].handle = invalid_handle_;
    --fd_pivot_;
    if (index < fd_pivot_) {
      unsigned other = fd_index_[fd_pivot_];
      assert(other < open_fds_.size());
      assert(open_fds_[other].handle != invalid_handle_);
      open_fds_[other].index = index;
      fd_index_[index] = other;
      fd_index_[fd_pivot_] = fd;
    }
    return 0;
  }

  unsigned GetMaxFds() const { return fd_index_.size(); }

 private:
  struct FdWrapper {
    FdWrapper(HandleT h, unsigned i) : handle(h), index(i) { }

    HandleT handle;
    /**
     * Back-pointer into fd_index_, which is needed when closing a file.
     */
    unsigned index;
  };


  inline bool IsValid(int fd) {
    if ((fd < 0) || (static_cast<unsigned>(fd) >= open_fds_.size()))
      return false;
    return open_fds_[fd].handle != invalid_handle_;
  }

  /**
   * An unused (available) file descriptor.
   */
  HandleT invalid_handle_;
  /**
   * The index of the first available file descriptor in fd_index_.
   */
  unsigned fd_pivot_;
  /**
   * Maps into open_fds_.  Until fd_pivot_, file descriptors are used.  As of
   * fd_pivot_, points to free file descriptors.  Used to acquire new file
   * descriptors in constant time.
   */
  std::vector<unsigned> fd_index_;
  /**
   * The file descriptor number mapped to a user-defined file descriptor
   * (struct).  The fd integer passed to users of the file descriptor table
   * points into this array.
   */
  std::vector<FdWrapper> open_fds_;
};  // class FdTable

#endif  // CVMFS_FD_TABLE_H_
