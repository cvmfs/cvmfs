/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_MGR_H_
#define CVMFS_BUNDLE_MGR_H_
#include <string>

#include "duplex_fuse.h"   // fuse_ino_t
#include "file_bundle.h"   // BundleFileMgr
#include "mountpoint.h"    //MountPoint*, FileSystem*
#include "shortstring.h"   // GetParentPath, GetFileName
#include "util/inode.h"    // GetPathForInode, GetDirentForInode
#include "util/pointer.h"  // UniquePtr

class BundleMgr {
 public:
  BundleMgr(MountPoint *mp, fuse_ino_t ino);
  void Fetch();
  explicit operator bool() const { return is_valid_; }

 private:
  static void *DoFetch(void *data);

  catalog::DirectoryEntry dirent_;
  PathString path_;
  NameString fname_;
  PathString parent_path_;
  PathString bundle_file_path_;
  UniquePtr<BundleFileMgr> bfm_;
  bool is_valid_;
};
#endif  // CVMFS_BUNDLE_MGR_H_

