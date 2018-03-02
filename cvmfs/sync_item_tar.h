/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_TAR_H_
#define CVMFS_SYNC_ITEM_TAR_H_

#include "sync_union.h"

namespace publish {

class SyncItemTar : public SyncItem {
 public:
  SyncItemTar(const string &relative_parent_path, const string &filename,
              struct archive *archive, struct archive_entry *entry,
              const SyncUnion *union_engine);

  SyncItemType GetScratchFiletype() const;
  catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;

  inline unsigned int GetRdevMajor() const { return major(stat_->st_rdev); }

  inline unsigned int GetRdevMinor() const { return minor(stat_->st_rdev); }

 private:
  const struct stat *stat_;
};

SyncItemTar::SyncItemTar(const string &relative_parent_path,
                         const string &filename, struct archive *archive,
                         struct archive_entry *entry,
                         const SyncUnion *union_engine)
    : SyncItem(relative_parent_path, filename, union_engine) {
  switch (archive_entry_filetype(entry)) {
    case AE_IFREG: {
      scratch_type_ = kItemFile;
      break;
    }
    case AE_IFLNK: {
      scratch_type_ = kItemSymlink;
      break;
    }
    case AE_IFSOCK: {
      scratch_type_ = kItemSocket;
      break;
    }
    case AE_IFCHR: {
      scratch_type_ = kItemCharacterDevice;
      break;
    }
    case AE_IFBLK: {
      scratch_type_ = kItemBlockDevice;
      break;
    }
    case AE_IFDIR: {
      scratch_type_ = kItemDir;
      break;
    }
    case AE_IFIFO: {
      scratch_type_ = kItemFifo;
      break;
    }
  }
  stat_ = archive_entry_stat(entry);
}

SyncItemType SyncItemTar::GetScratchFiletype() const { return scratch_type_; }

catalog::DirectoryEntryBase SyncItemTar::CreateBasicCatalogDirent() const {
  catalog::DirectoryEntryBase dirent;

  // inode and parent inode is determined at runtime of client
  dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;

  // this might mask the actual link count in case hardlinks are not supported
  // (i.e. on setups using OverlayFS)
  dirent.linkcount_ = this->stat_->st_nlink;

  dirent.mode_ = this->stat_->st_mode;
  dirent.uid_ = this->stat_->st_uid;
  dirent.gid_ = this->stat_->st_gid;
  dirent.size_ = this->stat_->st_size;
  dirent.mtime_ = this->stat_->st_mtime;
  dirent.checksum_ = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

  dirent.name_.Assign(this->filename().data(), this->filename().length());

  if (this->IsSymlink()) {
    char slnk[PATH_MAX + 1];
    const ssize_t length =
        readlink((this->GetUnionPath()).c_str(), slnk, PATH_MAX);
    assert(length >= 0);
    dirent.symlink_.Assign(slnk, length);
  }

  if (this->IsCharacterDevice() || this->IsBlockDevice()) {
    dirent.size_ = makedev(GetRdevMajor(), GetRdevMinor());
  }

  return dirent;
}
}

#endif  // CVMFS_SYNC_ITEM_TAR_H_
