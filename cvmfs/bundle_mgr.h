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
#include "util/pointer.h"  // UniquePtr

namespace cvmfs {
/*
 * functions defined inside /cvmfs/cvmfs.cc
 */
bool GetPathForInode(const fuse_ino_t ino, PathString *path);
bool GetDirentForInode(const fuse_ino_t ino, catalog::DirectoryEntry *dirent);
}  // namespace cvmfs

class BundleMgr {
 public:
  BundleMgr(MountPoint *mp, FileSystem *fs, fuse_ino_t ino) : is_valid_(true) {
    is_valid_ = cvmfs::GetPathForInode(ino, &path_);
    is_valid_ &= cvmfs::GetDirentForInode(ino, &dirent_);
    fname_ = GetFileName(path_);
    parent_path_ = GetParentPath(path_);

    // There is a naming convention regarding the name of the file with the
    // contents of the bundle
    bundle_file_path_ = PathString(parent_path_.ToString() + "/.cvmfsbundle."
                                   + fname_.ToString());

    bfm_ = new BundleFileMgr(bundle_file_path_);
  }

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

