/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_UNLINK_GUARD_H_
#define CVMFS_UTIL_UNLINK_GUARD_H_

#include <unistd.h>

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
  enum InitialState { kEnabled, kDisabled };

 public:
  inline UnlinkGuard() : enabled_(false) {}
  inline UnlinkGuard(const std::string &path,
                     const InitialState state = kEnabled)
            : path_(path)
            , enabled_(state == kEnabled) {}
  inline ~UnlinkGuard() { if (IsEnabled()) unlink(path_.c_str()); }

  inline void Set(const std::string &path) { path_ = path; Enable(); }

  inline bool IsEnabled() const { return enabled_;  }
  inline void Enable()          { enabled_ = true;  }
  inline void Disable()         { enabled_ = false; }

  const std::string& path() const { return path_; }

 private:
  std::string  path_;
  bool         enabled_;
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_UNLINK_GUARD_H_
