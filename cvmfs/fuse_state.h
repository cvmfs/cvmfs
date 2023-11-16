/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_STATE_H_
#define CVMFS_FUSE_STATE_H_

namespace cvmfs {

/**
 * Options related to the fuse kernel connection. The capabilities are
 * determined only once at mount time. If the capability trigger certain
 * behavior of the cvmfs fuse module, it needs to be re-triggered on reload.
 * Used in SaveState and RestoreState to store the details of symlink caching.
 */
struct FuseState {
  FuseState() : version(0), cache_symlinks(false), has_dentry_expire(false) {}
  unsigned version;
  bool cache_symlinks;
  bool has_dentry_expire;
};

}  // namespace cvmfs

#endif  // CVMFS_FUSE_STATE_H_
