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

#include <pthread.h>
#include <map>
#include <set>
#include <string>

#include "archive.h"
#include "sync_mediator.h"

namespace publish {

class SyncUnionTarball : public SyncUnion {
 public:
  SyncUnionTarball(AbstractSyncMediator *mediator,
                   const std::string &rdonly_path,
                   const std::string &union_path,
                   const std::string &scratch_path,
                   const std::string &tarball_path,
                   const std::string &base_directory,
                   const std::string &to_delete);

  ~SyncUnionTarball();

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

  std::string UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const;
  bool IsOpaqueDirectory(SharedPtr<SyncItem> directory) const;
  bool IsWhiteoutEntry(SharedPtr<SyncItem> entry) const;

 private:
  struct archive *src;
  const std::string tarball_path_;
  const std::string base_directory_;
  const std::string to_delete_;
  std::set<std::string> know_directories_;
  std::set<std::string> to_create_catalog_dirs_;
  std::map<std::string, SharedPtr<SyncItem> > dirs_;
  pthread_mutex_t* archive_lock_;
  pthread_cond_t* read_archive_cond_;
  bool* can_read_archive_;

  void CreateDirectories(const std::string &target);
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
