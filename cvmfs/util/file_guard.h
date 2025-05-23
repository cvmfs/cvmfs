/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_FILE_GUARD_H_
#define CVMFS_UTIL_FILE_GUARD_H_

#include <unistd.h>

#include <cstdio>
#include <string>

#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


/**
 * RAII object to call `unlink()` on a containing file when it gets out of scope
 */
class UnlinkGuard : SingleCopy {
 public:
  enum InitialState {
    kEnabled,
    kDisabled
  };

  inline UnlinkGuard() : enabled_(false) { }
  inline explicit UnlinkGuard(const std::string &path,
                              const InitialState state = kEnabled)
      : path_(path), enabled_(state == kEnabled) { }
  inline ~UnlinkGuard() {
    if (IsEnabled())
      unlink(path_.c_str());
  }

  inline void Set(const std::string &path) {
    path_ = path;
    Enable();
  }

  inline bool IsEnabled() const { return enabled_; }
  inline void Enable() { enabled_ = true; }
  inline void Disable() { enabled_ = false; }

  const std::string &path() const { return path_; }

 private:
  std::string path_;
  bool enabled_;
};


/**
 * RAII object to close a file descriptor when it gets out of scope
 */
class FdGuard : SingleCopy {
 public:
  inline FdGuard() : fd_(-1) { }
  explicit inline FdGuard(const int fd) : fd_(fd) { }
  inline ~FdGuard() {
    if (fd_ >= 0)
      close(fd_);
  }
  int fd() const { return fd_; }

 private:
  int fd_;
};


/**
 * RAII object to close a FILE stream when it gets out of scope
 */
class FileGuard : SingleCopy {
 public:
  inline FileGuard() : file_(NULL) { }
  explicit inline FileGuard(FILE *file) : file_(file) { }
  inline ~FileGuard() {
    if (file_ != NULL)
      fclose(file_);
  }
  const FILE *file() const { return file_; }

 private:
  FILE *file_;
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_FILE_GUARD_H_
