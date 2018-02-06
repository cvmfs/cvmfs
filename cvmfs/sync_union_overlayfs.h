/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion` to provide support for
 * overlayfs
 *
 */

#ifndef CVMFS_SYNC_UNION_OVERLAYFS_H_
#define CVMFS_SYNC_UNION_OVERLAYFS_H_

#include "sync_union.h"

#include <set>
#include <string>

namespace publish {

/**
 * Syncing a cvmfs repository by the help of an overlayed overlayfs
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
  void PreprocessSyncItem(SyncItem *entry) const;

  bool IsWhiteoutEntry(const SyncItem &entry) const;
  bool IsOpaqueDirectory(const SyncItem &directory) const;
  bool IsWhiteoutSymlinkPath(const std::string &path) const;

  std::string UnwindWhiteoutFilename(const SyncItem &entry) const;
  std::set<std::string> GetIgnoreFilenames() const;

  void CheckForBrokenHardlink(const SyncItem &entry) const;
  void MaskFileHardlinks(SyncItem *entry) const;

  bool ObtainSysAdminCapability() const;

 private:
  bool IsOpaqueDirPath(const std::string &path) const;

  std::set<std::string> hardlink_lower_files_;
  uint64_t hardlink_lower_inode_;
};  // class SyncUnionOverlayfs
}  // namespace publish

#endif  // CVMFS_SYNC_UNION_OVERLAYFS_H_
