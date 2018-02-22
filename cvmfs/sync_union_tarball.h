/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion` to
 * provide support for tarballs
 *
 */

#ifndef CVMFS_SYNC_UNION_TARBALL_H_
#define CVMFS_SYNC_UNION_TARBALL_H_

#include "sync_union.h"

#include <set>
#include <string>

#include "sync_mediator.h"

#include "archive.h"

struct archive;

namespace publish {

class SyncUnionTarball : public SyncUnion {
 public:
  SyncUnionTarball(AbstractSyncMediator *mediator,
                   const std::string &rdonly_path,
                   const std::string &union_path,
                   const std::string &scratch_path,
                   const std::string &tarball_path,
                   const std::string &base_directory);

  /*
   * Delete the working directories, where the tar is being uncompressed.
   */
  // ~SyncUnionTarball();

  /*
   * Check that the tarball is actually valid and that can be open.
   * Similarly it creates the base directory or check that it has the ability to
   * write in it.
   * Finally it untar the tarball, recursively.
   */
  bool Initialize();

  /*
   * Simply create the SyncItem from the basedirectory opened
   */
  void Traverse();

  std::string UnwindWhiteoutFilename(const SyncItem &entry) const;
  bool IsOpaqueDirectory(const SyncItem &directory) const;
  bool IsWhiteoutEntry(const SyncItem &entry) const;

 private:
  const std::string tarball_path_;
  const std::string base_directory_;
  std::string working_dir_;

  /*
   * Actually untar the several elements in the tar inside the base directory,
   * it returns all the recursive tars find in this operation
   */
  bool UntarPath(const std::string &base_untar_directory_path,
                 const std::string &tarball_path);

  /*
   * Helper function to phisically move the data from the source to the
   * destination.
   */
  int CopyData(struct archive *src, struct archive *dst);
};  // class SyncUnionTarball

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_TARBALL_H_
