/**
 * This file is part of the CernVM File System
 */

#include "sync_item_tar.h"

#include <string>

#include "directory_entry.h"
#include "duplex_libarchive.h"
#include "ingestion/ingestion_source.h"
#include "sync_union_tarball.h"
#include "util/concurrency.h"
#include "util/platform.h"

namespace publish {

SyncItemTar::SyncItemTar(const std::string &relative_parent_path,
                         const std::string &filename, struct archive *archive,
                         struct archive_entry *entry,
                         Signal *read_archive_signal,
                         const SyncUnion *union_engine, const uid_t uid,
                         const gid_t gid)
    : SyncItem(relative_parent_path, filename, union_engine, kItemUnknown)
    , archive_(archive)
    , archive_entry_(entry)
    , obtained_tar_stat_(false)
    , read_archive_signal_(read_archive_signal)
    , uid_(uid)
    , gid_(gid) {
  GetStatFromTar();
}

SyncItemTar::SyncItemTar(const std::string &relative_parent_path,
                         const std::string &filename, struct archive *archive,
                         struct archive_entry *entry,
                         Signal *read_archive_signal,
                         const SyncUnion *union_engine)
    : SyncItem(relative_parent_path, filename, union_engine, kItemUnknown)
    , archive_(archive)
    , archive_entry_(entry)
    , obtained_tar_stat_(false)
    , read_archive_signal_(read_archive_signal)
    , uid_(-1u)
    , gid_(-1u) {
  GetStatFromTar();
}

void SyncItemTar::StatScratch(const bool refresh) const {
  if (scratch_stat_.obtained && !refresh)
    return;
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

bool SyncItemTar::IsType(const SyncItemType expected_type) const {
  if (scratch_type_ == kItemUnknown) {
    scratch_type_ = GetScratchFiletype();
  }
  return scratch_type_ == expected_type;
}

platform_stat64 SyncItemTar::GetStatFromTar() const {
  assert(archive_entry_);
  if (obtained_tar_stat_)
    return tar_stat_;

  const struct stat *entry_stat = archive_entry_stat(archive_entry_);
  assert(NULL != entry_stat);

  tar_stat_.st_mode = entry_stat->st_mode;

  if (uid_ != -1u) {
    tar_stat_.st_uid = uid_;
  } else {
    tar_stat_.st_uid = entry_stat->st_uid;
  }
  if (gid_ != -1u) {
    tar_stat_.st_gid = gid_;
  } else {
    tar_stat_.st_gid = entry_stat->st_gid;
  }

  tar_stat_.st_rdev = entry_stat->st_rdev;
  tar_stat_.st_size = entry_stat->st_size;
  tar_stat_.st_mtime = entry_stat->st_mtime;
#ifdef __APPLE__
  tar_stat_.st_mtimespec.tv_nsec = entry_stat->st_mtimespec.tv_nsec;
#else
  tar_stat_.st_mtim.tv_nsec = entry_stat->st_mtim.tv_nsec;
#endif
  tar_stat_.st_nlink = entry_stat->st_nlink;

  if (IsDirectory()) {
    tar_stat_.st_size = 4096;
  }

  obtained_tar_stat_ = true;

  return tar_stat_;
}

catalog::DirectoryEntryBase SyncItemTar::CreateBasicCatalogDirent(
    bool enable_mtime_ns) const {
  assert(obtained_tar_stat_);

  catalog::DirectoryEntryBase dirent;

  // inode and parent inode is determined at runtime of client
  dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;

  // tarfiles do not keep information about the linkcount, so it should always
  // appear as zero
  assert(this->tar_stat_.st_nlink == 0);
  dirent.linkcount_ = 1;

  dirent.mode_ = this->tar_stat_.st_mode;
  dirent.uid_ = this->tar_stat_.st_uid;
  dirent.gid_ = this->tar_stat_.st_gid;
  dirent.size_ = this->tar_stat_.st_size;
  dirent.mtime_ = this->tar_stat_.st_mtime;
  dirent.checksum_ = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

  dirent.name_.Assign(this->filename().data(), this->filename().length());

  if (this->IsSymlink()) {
    const std::string symlink(archive_entry_symlink(archive_entry_));
    dirent.symlink_.Assign(symlink.c_str(), symlink.length());
  }

  if (this->IsCharacterDevice() || this->IsBlockDevice()) {
    dirent.size_ = makedev(major(tar_stat_.st_rdev), minor(tar_stat_.st_rdev));
  }

  if (enable_mtime_ns) {
#ifdef __APPLE__
    dirent.mtime_ns_ = static_cast<int32_t>(
        this->tar_stat_.st_mtimespec.tv_nsec);
#else
    dirent.mtime_ns_ = static_cast<int32_t>(this->tar_stat_.st_mtim.tv_nsec);
#endif
  }

  assert(dirent.IsRegular() || dirent.IsDirectory() || dirent.IsLink()
         || dirent.IsSpecial());

  return dirent;
}

IngestionSource *SyncItemTar::CreateIngestionSource() const {
  return new TarIngestionSource(GetUnionPath(), archive_, archive_entry_,
                                read_archive_signal_);
}
}  // namespace publish
