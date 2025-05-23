/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion` to
 * provide support for tarballs
 *
 */

#ifndef CVMFS_SYNC_UNION_TARBALL_H_
#define CVMFS_SYNC_UNION_TARBALL_H_

#include <pthread.h>

#include <list>
#include <map>
#include <set>
#include <string>

#include "duplex_libarchive.h"
#include "sync_union.h"
#include "util/concurrency.h"

namespace publish {

class AbstractSyncMediator;

class SyncUnionTarball : public SyncUnion {
 public:
  SyncUnionTarball(AbstractSyncMediator *mediator,
                   const std::string &rdonly_path,
                   const std::string &tarball_path,
                   const std::string &base_directory,
                   const uid_t uid,
                   const gid_t gid,
                   const std::string &to_delete,
                   const bool create_catalog_on_root,
                   const std::string &path_delimiter = ":");

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

  void PostUpload();

  std::string UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const;
  bool IsOpaqueDirectory(SharedPtr<SyncItem> directory) const;
  bool IsWhiteoutEntry(SharedPtr<SyncItem> entry) const;

 private:
  struct archive *src;
  const std::string tarball_path_;
  const std::string base_directory_;
  const uid_t uid_;
  const gid_t gid_;
  const std::string to_delete_;  ///< entity to delete before to extract the tar
  const bool create_catalog_on_root_;
  const std::string path_delimiter_;  ///< delimiter used to split paths
  std::set<std::string>
      know_directories_;  ///< directory that we know already exist

  /**
   * directories where we found catalog marker, after the main traverse we
   * iterate through them and we add the catalog
   */
  std::set<std::string> to_create_catalog_dirs_;

  /**
   * map of all directories found, we need them since we don't know, at priori,
   * where the catalog files appears
   */
  std::map<std::string, SharedPtr<SyncItem> > dirs_;

  /**
   * map all the file that point to the same hardlink to the path of the file
   * itself
   */
  std::map<const std::string, std::list<std::string> > hardlinks_;

  /**
   * Conditional variable to keep track of when is possible to read the tar file
   */
  Signal *read_archive_signal_;

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
  void ProcessArchiveEntry(struct archive_entry *entry);
  std::string SanitizePath(const std::string &path);
};  // class SyncUnionTarball

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_TARBALL_H_
