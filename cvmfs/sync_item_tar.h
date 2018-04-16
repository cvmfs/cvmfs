/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_TAR_H_
#define CVMFS_SYNC_ITEM_TAR_H_

#include "sync_union.h"

#include <pthread.h>

#include <string>

#include "duplex_libarchive.h"
#include "util_concurrency.h"

namespace publish {

class SyncItemTar : public SyncItem {
  friend class SyncUnionTarball;

 public:
  SyncItemType GetScratchFiletype() const;
  catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;

  void StatScratch(const bool refresh = false) const;

  inline unsigned int GetRdevMajor() const { return major(tar_stat_.st_rdev); }
  inline unsigned int GetRdevMinor() const { return minor(tar_stat_.st_rdev); }

  IngestionSource *CreateIngestionSource() const;
  void AlreadyCreatedDir() const { rdonly_type_ = kItemDir; }

  struct archive *archive_;
  struct archive_entry *archive_entry_;

 protected:
  SyncItemTar(const string &relative_parent_path, const string &filename,
              struct archive *archive, struct archive_entry *entry,
              Signal *read_archive_signal, const SyncUnion *union_engine);

 private:
  platform_stat64 GetStatFromTar() const;
  mutable platform_stat64 tar_stat_;
  mutable bool obtained_tar_stat_;
  Signal *read_archive_signal_;
};

SyncItemTar::SyncItemTar(const string &relative_parent_path,
                         const string &filename, struct archive *archive,
                         struct archive_entry *entry,
                         Signal *read_archive_signal,
                         const SyncUnion *union_engine)
    : SyncItem(relative_parent_path, filename, union_engine, kItemTarfile),
      archive_(archive),
      archive_entry_(entry),
      obtained_tar_stat_(false),
      read_archive_signal_(read_archive_signal) {
  GetStatFromTar();
}

void SyncItemTar::StatScratch(const bool refresh) const {
  if (scratch_stat_.obtained && !refresh) return;
  scratch_stat_.stat = GetStatFromTar();
  scratch_stat_.error_code = 0;
  scratch_stat_.obtained = true;
}

SyncItemType SyncItemTar::GetScratchFiletype() const {
  assert(archive_entry_);
  switch (archive_entry_filetype(archive_entry_)) {
    case AE_IFREG: {
      return kItemFile;
      break;
    }
    case AE_IFLNK: {
      return kItemSymlink;
      break;
    }
    case AE_IFSOCK: {
      return kItemSocket;
      break;
    }
    case AE_IFCHR: {
      return kItemCharacterDevice;
      break;
    }
    case AE_IFBLK: {
      return kItemBlockDevice;
      break;
    }
    case AE_IFDIR: {
      return kItemDir;
      break;
    }
    case AE_IFIFO: {
      return kItemFifo;
      break;
    }
    default:
      return kItemUnknown;
      break;
  }
}

platform_stat64 SyncItemTar::GetStatFromTar() const {
  assert(archive_entry_);
  if (obtained_tar_stat_) return tar_stat_;

  const struct stat *entry_stat_ = archive_entry_stat(archive_entry_);

  tar_stat_.st_mode = entry_stat_->st_mode;
  tar_stat_.st_uid = entry_stat_->st_uid;
  tar_stat_.st_gid = entry_stat_->st_gid;
  tar_stat_.st_size = entry_stat_->st_size;
  tar_stat_.st_mtime = entry_stat_->st_mtime;
  tar_stat_.st_nlink = entry_stat_->st_nlink;

  if (IsDirectory()) {
    tar_stat_.st_size = 4096;
  }

  obtained_tar_stat_ = true;

  return tar_stat_;
}

catalog::DirectoryEntryBase SyncItemTar::CreateBasicCatalogDirent() const {
  assert(archive_entry_);
  assert(obtained_tar_stat_);

  catalog::DirectoryEntryBase dirent;

  // inode and parent inode is determined at runtime of client
  dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;

  dirent.linkcount_ = this->tar_stat_.st_nlink;

  dirent.mode_ = this->tar_stat_.st_mode;
  dirent.uid_ = this->tar_stat_.st_uid;
  dirent.gid_ = this->tar_stat_.st_gid;
  dirent.size_ = this->tar_stat_.st_size;
  dirent.mtime_ = this->tar_stat_.st_mtime;
  dirent.checksum_ = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

  dirent.name_.Assign(this->filename().data(), this->filename().length());

  /*
  if (this->IsSymlink()) {
    std::string symlink(archive_entry_symlink(archive_entry_));
    dirent.symlink_.Assign(symlink.c_str(), symlink.length());
  }
  */

  if (this->IsCharacterDevice() || this->IsBlockDevice()) {
    dirent.size_ = makedev(GetRdevMajor(), GetRdevMinor());
  }

  if (dirent.IsDirectory() && dirent.size_ == 0) dirent.size_ = 4096;

  assert(dirent.IsRegular() || dirent.IsDirectory() || dirent.IsLink() ||
         dirent.IsSpecial());

  return dirent;
}

IngestionSource *SyncItemTar::CreateIngestionSource() const {
  return new TarIngestionSource(GetUnionPath(), archive_, archive_entry_,
                                read_archive_signal_);
}
}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_TAR_H_
