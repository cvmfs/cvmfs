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

#include "duplex_libarchive.h"
#include "util_concurrency.h"

namespace publish {

class AbstractSyncMediator;

class SyncUnionTarball : public SyncUnion {
 public:
  SyncUnionTarball(AbstractSyncMediator *mediator,
                   const std::string &rdonly_path,
                   const std::string &tarball_path,
                   const std::string &base_directory,
                   const std::string &to_delete);

  ~SyncUnionTarball();

  /*
   * Check that the tarball is actually valid and that can be open.
   */
  bool Initialize();

  /*
   * We start by deleting the entity that we are request to delete.
   * Then we move on to extracting the tarball.
   * For each directory we found we remember it associated with its SyncItem on
   * the `dirs_` map.
   * Similarly we remember where nested catalogs should be placed in
   * `to_create_catalog_dirs_`.
   * After we finish to uncompress the tarball we come back to iterate over
   * `to_create_catalog_dirs_` and we created the nested catalogs.
   */
  void Traverse();

  std::string UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const;
  bool IsOpaqueDirectory(SharedPtr<SyncItem> directory) const;
  bool IsWhiteoutEntry(SharedPtr<SyncItem> entry) const;

 private:
  struct archive *src;
  const std::string tarball_path_;
  const std::string base_directory_;
  const std::string to_delete_;  ///< entity to delete before to extract the tar
  std::set<std::string>
      know_directories_;  ///< directory that we know already exist
  std::set<std::string> to_create_catalog_dirs_;
  ///< directories where we found catalog marker, after the main traverse we
  ///< iterate through them and we add the catalog
  std::map<std::string, SharedPtr<SyncItem> > dirs_;
  ///< map of all directories found, we need them since we don't know, at
  ///< priori, where the catalog files appears
  Signal *read_archive_signal_;  ///< Conditional variable to keep track of when
                                 ///< is possible to read the tar file

  static const size_t kBlockSize = 4096 * 4;

  /**
   * create missing directory and all the ancestors
   * It is possible to find the leaf of the filesystem tree before than its root
   * while
   * traversing a tar file, however we need to have all the directories in place
   * before adding entities. This method is called whener we find a new
   * directory.
   * The method create a new dummy directory and, if necessary, all of its
   * parents.
   * @param target the directory to create
   */
  void CreateDirectories(const std::string &target);
};  // class SyncUnionTarball

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_TARBALL_H_
