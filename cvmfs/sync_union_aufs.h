/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion`
 * to provide support for AUFS
 *
 */

#ifndef CVMFS_SYNC_UNION_AUFS_H_
#define CVMFS_SYNC_UNION_AUFS_H_

#include <set>
#include <string>

#include "sync_union.h"

namespace publish {
/**
 * Syncing a cvmfs repository by the help of an overlaid AUFS
 * read-write volume.
 */
class SyncUnionAufs : public SyncUnion {
 public:
  SyncUnionAufs(SyncMediator *mediator, const std::string &rdonly_path,
                const std::string &union_path, const std::string &scratch_path);

  void Traverse();
  bool SupportsHardlinks() const { return true; }

 protected:
  bool IsWhiteoutEntry(SharedPtr<SyncItem> entry) const;
  bool IsOpaqueDirectory(SharedPtr<SyncItem> directory) const;
  bool IgnoreFilePredicate(const std::string &parent_dir,
                           const std::string &filename);
  std::string UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const;

 private:
  std::set<std::string> ignore_filenames_;
  std::string whiteout_prefix_;
};  // class SyncUnionAufs
}  // namespace publish
#endif  // CVMFS_SYNC_UNION_AUFS_H_
