/**
 * This file is part of the CernVM file system
 */

#include "sync_item.h"

#include <errno.h>

#include "sync_mediator.h"

using namespace std;  // NOLINT

namespace publish {

SyncItem::SyncItem(const string &relative_parent_path,
                   const string &filename,
                   const SyncItemType entry_type,
                   const SyncUnion *union_engine) :
  type_(entry_type),
  whiteout_(false),
  relative_parent_path_(relative_parent_path),
  filename_(filename),
  union_engine_(union_engine)
{
  content_hash_.algorithm = shash::kAny;
}


bool SyncItem::IsNew() const {
  // Careful: this can also mean delete + recreate
  StatRdOnly();
  return (rdonly_stat_.error_code == ENOENT);
}


void SyncItem::MarkAsWhiteout(const std::string &actual_filename) {
  // Mark the file as whiteout entry and strip the whiteout prefix
  whiteout_ = true;
  filename_ = actual_filename;

  // Find the entry in the repository
  StatRdOnly();
  if (rdonly_stat_.error_code != 0) {
    PrintWarning("'" + GetRelativePath() + "' should be deleted, but was not"
                 "found in repository.");
    return;
  }

  // What is deleted?
  if (S_ISDIR(rdonly_stat_.stat.st_mode)) type_ = kItemDir;
  else if (S_ISREG(rdonly_stat_.stat.st_mode)) type_ = kItemFile;
  else if (S_ISLNK(rdonly_stat_.stat.st_mode)) type_ = kItemSymlink;
}


unsigned int SyncItem::GetRdOnlyLinkcount() const {
  StatRdOnly();
  return rdonly_stat_.stat.st_nlink;
}


uint64_t SyncItem::GetRdOnlyInode() const {
  StatRdOnly();
  return rdonly_stat_.stat.st_ino;
}


unsigned int SyncItem::GetUnionLinkcount() const {
  StatUnion();
  return union_stat_.stat.st_nlink;
}


uint64_t SyncItem::GetUnionInode() const {
  StatUnion();
  return union_stat_.stat.st_ino;
}


void SyncItem::StatGeneric(const string &path, EntryStat *info) {
  int retval = platform_lstat(path.c_str(), &info->stat);
  if (retval != 0)
    info->error_code = errno;
  info->obtained = true;
}


bool SyncItem::IsOpaqueDirectory() const {
  if (!IsDirectory()) {
    return false;
  }
  return union_engine_->IsOpaqueDirectory(*this);
}


catalog::DirectoryEntryBase SyncItem::CreateBasicCatalogDirent() const {
  catalog::DirectoryEntryBase dirent;

  // inode and parent inode is determined at runtime of client
  dirent.inode_          = catalog::DirectoryEntry::kInvalidInode;
  dirent.parent_inode_   = catalog::DirectoryEntry::kInvalidInode;
  dirent.linkcount_      = this->GetUnionStat().st_nlink; // TODO: is this a good idea here?

  dirent.mode_           = this->GetUnionStat().st_mode;
  dirent.uid_            = this->GetUnionStat().st_uid;
  dirent.gid_            = this->GetUnionStat().st_gid;
  dirent.size_           = this->GetUnionStat().st_size;
  dirent.mtime_          = this->GetUnionStat().st_mtime;
  dirent.checksum_       = this->GetContentHash();

  dirent.name_.Assign(filename_.data(), filename_.length());

  if (this->IsSymlink()) {
    char slnk[PATH_MAX+1];
		const ssize_t length = readlink((this->GetUnionPath()).c_str(), slnk, PATH_MAX);
    assert(length >= 0);
    dirent.symlink_.Assign(slnk, length);
  }

  return dirent;
}

std::string SyncItem::GetRdOnlyPath() const {
  const string relative_path = GetRelativePath().empty() ?
                               "" : "/" + GetRelativePath();
  return union_engine_->rdonly_path() + relative_path;
}

std::string SyncItem::GetUnionPath() const {
  const string relative_path = GetRelativePath().empty() ?
                               "" : "/" + GetRelativePath();
  return union_engine_->union_path() + relative_path;
}

std::string SyncItem::GetScratchPath() const {
  const string relative_path = GetRelativePath().empty() ?
                               "" : "/" + GetRelativePath();
  return union_engine_->scratch_path() + relative_path;
}

}  // namespace sync
