/**
 * This file is part of the CernVM file system
 */

#include "sync_item.h"

#include <errno.h>

#include "sync_mediator.h"
#include "sync_union.h"

using namespace std;  // NOLINT

namespace publish {


SyncItem::SyncItem() :
  union_engine_(NULL),
  whiteout_(false),
  opaque_(false),
  masked_hardlink_(false),
  scratch_type_(static_cast<SyncItemType>(0)),
  rdonly_type_(static_cast<SyncItemType>(0))
{
}

SyncItem::SyncItem(const string       &relative_parent_path,
                   const string       &filename,
                   const SyncUnion    *union_engine,
                   const SyncItemType  entry_type) :
  union_engine_(union_engine),
  whiteout_(false),
  opaque_(false),
  masked_hardlink_(false),
  relative_parent_path_(relative_parent_path),
  filename_(filename),
  scratch_type_(entry_type),
  rdonly_type_(kItemUnknown)
{
  content_hash_.algorithm = shash::kAny;
}


SyncItemType SyncItem::GetGenericFiletype(const SyncItem::EntryStat &stat) const
{
  const SyncItemType type = stat.GetSyncItemType();
  if (type == kItemUnknown) {
    PrintWarning("'" + GetRelativePath() + "' has an unsupported file type "
                 "(st_mode: " + StringifyInt(stat.stat.st_mode) +
                 " errno: " + StringifyInt(stat.error_code) + ")");
    abort();
  }
  return type;
}


SyncItemType SyncItem::GetRdOnlyFiletype() const {
  StatRdOnly();
  // file could not exist in read-only branch, or a regular file could have
  // been replaced by a directory in the read/write branch, like:
  // rdonly:
  //    /foo/bar/regular_file   <-- ENOTDIR when asking for (.../is_dir_now)
  // r/w:
  //    /foo/bar/regular_file/
  //    /foo/bar/regular_file/is_dir_now
  if (rdonly_stat_.error_code == ENOENT ||
      rdonly_stat_.error_code == ENOTDIR) return kItemNew;
  return GetGenericFiletype(rdonly_stat_);
}


SyncItemType SyncItem::GetScratchFiletype() const {
  StatScratch();
  if (scratch_stat_.error_code != 0) {
    PrintWarning("Failed to stat() '" + GetRelativePath() + "' in scratch. "
                 "(errno: " + StringifyInt(scratch_stat_.error_code) + ")");
    abort();
  }

  return GetGenericFiletype(scratch_stat_);
}


void SyncItem::MarkAsWhiteout(const std::string &actual_filename) {
  // Mark the file as whiteout entry and strip the whiteout prefix
  whiteout_ = true;
  filename_ = actual_filename;

  // Find the entry in the repository
  StatRdOnly(true);  // <== refreshing the stat (filename might have changed)
  if (rdonly_stat_.error_code != 0) {
    PrintWarning("'" + GetRelativePath() + "' should be deleted, but was not "
                 "found in repository.");
    abort();
    return;
  }

  // What is deleted?
  rdonly_type_  = GetRdOnlyFiletype();
  scratch_type_ = GetRdOnlyFiletype();
}


void SyncItem::MarkAsOpaqueDirectory() {
  assert(IsDirectory());
  opaque_ = true;
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


void SyncItem::StatGeneric(const string  &path,
                           EntryStat     *info,
                           const bool     refresh) {
  if (info->obtained && !refresh) return;
  int retval = platform_lstat(path.c_str(), &info->stat);
  info->error_code = (retval != 0) ? errno : 0;
  info->obtained = true;
}


catalog::DirectoryEntryBase SyncItem::CreateBasicCatalogDirent() const {
  catalog::DirectoryEntryBase dirent;

  // inode and parent inode is determined at runtime of client
  dirent.inode_          = catalog::DirectoryEntry::kInvalidInode;
  dirent.parent_inode_   = catalog::DirectoryEntry::kInvalidInode;

  // this might mask the actual link count in case hardlinks are not supported
  // (i.e. on setups using OverlayFS)
  dirent.linkcount_      = HasHardlinks() ? this->GetUnionStat().st_nlink : 1;

  dirent.mode_           = this->GetUnionStat().st_mode;
  dirent.uid_            = this->GetUnionStat().st_uid;
  dirent.gid_            = this->GetUnionStat().st_gid;
  dirent.size_           = this->GetUnionStat().st_size;
  dirent.mtime_          = this->GetUnionStat().st_mtime;
  dirent.checksum_       = this->GetContentHash();
  dirent.compression_alg_= this->GetCompressionAlg();

  dirent.name_.Assign(filename_.data(), filename_.length());

  if (this->IsSymlink()) {
    char slnk[PATH_MAX+1];
    const ssize_t length =
      readlink((this->GetUnionPath()).c_str(), slnk, PATH_MAX);
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

}  // namespace publish
