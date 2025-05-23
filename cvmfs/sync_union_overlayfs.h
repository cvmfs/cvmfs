/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion` to provide support for
 * overlayfs
 *
 */

#ifndef CVMFS_SYNC_UNION_OVERLAYFS_H_
#define CVMFS_SYNC_UNION_OVERLAYFS_H_

#include <set>
#include <string>

#include "sync_union.h"
#include "util/shared_ptr.h"

namespace publish {

/**
 * Syncing a cvmfs repository by the help of an overlaid overlayfs
 * read-write volume.
 */
class SyncUnionOverlayfs : public SyncUnion {
 public:
  SyncUnionOverlayfs(SyncMediator *mediator, const std::string &rdonly_path,
                     const std::string &union_path,
                     const std::string &scratch_path);

  bool Initialize();

  void Traverse();
  static bool ReadlinkEquals(std::string const &path,
                             std::string const &compare_value);
  static bool HasXattr(std::string const &path, std::string const &attr_name);

 protected:
  void PreprocessSyncItem(SharedPtr<SyncItem> entry) const;

  bool IsWhiteoutEntry(SharedPtr<SyncItem> entry) const;
  bool IsOpaqueDirectory(SharedPtr<SyncItem> directory) const;
  bool IsWhiteoutSymlinkPath(const std::string &path) const;

  std::string UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const;
  std::set<std::string> GetIgnoreFilenames() const;

  void CheckForBrokenHardlink(SharedPtr<SyncItem> entry) const;
  void MaskFileHardlinks(SharedPtr<SyncItem> entry) const;

  bool ObtainSysAdminCapability() const;

 private:
  bool IsOpaqueDirPath(const std::string &path) const;

  std::set<std::string> hardlink_lower_files_;
  uint64_t hardlink_lower_inode_;
};  // class SyncUnionOverlayfs
}  // namespace publish

#endif  // CVMFS_SYNC_UNION_OVERLAYFS_H_
