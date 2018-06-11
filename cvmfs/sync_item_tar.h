/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_TAR_H_
#define CVMFS_SYNC_ITEM_TAR_H_

#include "sync_item.h"

#include <string>

#include "directory_entry.h"
#include "duplex_libarchive.h"
#include "ingestion/ingestion_source.h"
#include "platform.h"
#include "sync_union_tarball.h"
#include "util_concurrency.h"

namespace publish {

class SyncItemTar : public SyncItem {
  friend class SyncUnionTarball;

 public:
  virtual catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;
  virtual IngestionSource *CreateIngestionSource() const;
  virtual void IsPlaceholderDirectory() const { rdonly_type_ = kItemDir; }
  virtual SyncItemType GetScratchFiletype() const;
  virtual bool IsType(const SyncItemType expected_type) const;
  virtual void StatScratch(const bool refresh = false) const;

 protected:
  SyncItemTar(const std::string &relative_parent_path,
              const std::string &filename, struct archive *archive,
              struct archive_entry *entry, Signal *read_archive_signal,
              const SyncUnion *union_engine);

 private:
  struct archive *archive_;
  struct archive_entry *archive_entry_;
  platform_stat64 GetStatFromTar() const;
  mutable platform_stat64 tar_stat_;
  mutable bool obtained_tar_stat_;
  Signal *read_archive_signal_;
};

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_TAR_H_
