/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_DUMMY_H_
#define CVMFS_SYNC_ITEM_DUMMY_H_

#include <ctime>
#include <string>

#include "ingestion/ingestion_source.h"
#include "sync_item.h"
#include "sync_union_tarball.h"

namespace publish {

class SyncItemDummyCatalog : public SyncItem {
  friend class SyncUnionTarball;

 protected:
  SyncItemDummyCatalog(const std::string &relative_parent_path,
                       const SyncUnion *union_engine)
      : SyncItem(relative_parent_path, ".cvmfscatalog", union_engine,
                 kItemFile) { }

 public:
  bool IsType(const SyncItemType expected_type) const {
    return expected_type == kItemFile;
  }

  catalog::DirectoryEntryBase CreateBasicCatalogDirent(
      bool /* enable_mtime_ns */) const {
    catalog::DirectoryEntryBase dirent;
    std::string name(".cvmfscatalog");
    dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;
    dirent.linkcount_ = 1;
    dirent.mode_ = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
    dirent.uid_ = getuid();
    dirent.gid_ = getgid();
    dirent.size_ = 0;
    dirent.mtime_ = time(NULL);
    dirent.checksum_ = this->GetContentHash();
    dirent.is_external_file_ = false;
    dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

    dirent.name_.Assign(name.data(), name.length());

    return dirent;
  }

  IngestionSource *CreateIngestionSource() const {
    return new StringIngestionSource("", GetUnionPath());
  }

  void StatScratch(const bool /* refresh */) const { return; }

  SyncItemType GetScratchFiletype() const { return kItemFile; }

  void MakePlaceholderDirectory() const { }
};

/*
 * This class represents dummy directories that we know are going to be there
 * but we still haven't found yet. This is possible in the extraction of
 * tarball, where the files are not extracted in order (root to leaves) but in a
 * random fashion.
 */
class SyncItemDummyDir : public SyncItemNative {
  friend class SyncUnionTarball;

 public:
  virtual catalog::DirectoryEntryBase CreateBasicCatalogDirent(
      bool enable_mtime_ns) const;
  SyncItemType GetScratchFiletype() const;
  virtual void MakePlaceholderDirectory() const { rdonly_type_ = kItemDir; }

 protected:
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type)
      : SyncItemNative(relative_parent_path, filename, union_engine,
                       entry_type) {
    assert(kItemDir == entry_type);

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = kPermision;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = getuid();
    scratch_stat_.stat.st_gid = getgid();
  }
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type, uid_t uid, gid_t gid)
      : SyncItemNative(relative_parent_path, filename, union_engine,
                       entry_type) {
    assert(kItemDir == entry_type);

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = kPermision;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = uid;
    scratch_stat_.stat.st_gid = gid;
  }

 private:
  static const mode_t kPermision = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR
                                   | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
};

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_DUMMY_H_
