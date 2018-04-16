/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_DUMMY_H_
#define CVMFS_SYNC_ITEM_DUMMY_H_

#include "sync_item.h"

#include <ctime>
#include <string>

#include "sync_union_tarball.h"

namespace publish {
/*
 * This class represents dummy directories that we know are going to be there
 * but we still haven't found yet. This is possible in the extraction of
 * tarball, where the files are not extracted in order (root to leaves) but in a
 * random fashion.
 */
class SyncItemDummyDir : public SyncItem {
  friend class SyncUnionTarball;

 public:
  catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;
  SyncItemType GetScratchFiletype() const;

 protected:
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type)
      : SyncItem(relative_parent_path, filename, union_engine, entry_type) {
    assert(kItemDir == entry_type);

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = kPermision;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = getuid();
    scratch_stat_.stat.st_gid = getgid();
  }

 private:
  mode_t kPermision = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP;
};

catalog::DirectoryEntryBase SyncItemDummyDir::CreateBasicCatalogDirent() const {
  catalog::DirectoryEntryBase dirent;

  dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;

  dirent.linkcount_ = 1;

  dirent.mode_ = kPermision;

  dirent.uid_ = getuid();
  dirent.gid_ = getgid();
  dirent.size_ = 4096;
  dirent.mtime_ = time(NULL);
  dirent.checksum_ = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

  dirent.name_.Assign(this->filename().data(), this->filename().length());

  assert(dirent.IsDirectory());

  return dirent;
}


SyncItemType SyncItemDummyDir::GetScratchFiletype() const {
  return kItemDir;
}

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_DUMMY_H_
