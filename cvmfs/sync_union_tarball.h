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
#include "sync_mediator.h"
#include "util_concurrency.h"

namespace publish {

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
  const std::string to_delete_; /* entity to delete */
  std::set<std::string>
      know_directories_; /* directory that we know already exist */
  std::set<std::string> to_create_catalog_dirs_;
  std::map<std::string, SharedPtr<SyncItem> > dirs_;
  Signal *read_archive_signal_;

  static const size_t kBlockSize = 4096 * 4;

  void CreateDirectories(const std::string &target);
  bool ProcessArchiveEntry(struct archive_entry *entry);
  void LogEntry(struct archive_entry *entry);
};  // class SyncUnionTarball

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_TARBALL_H_
