/**
 * This file is part of the CernVM file system
 */

#include "sync_item.h"

#include <errno.h>

#include "sync_mediator.h"

using namespace std;  // NOLINT

namespace publish {


SyncItem::SyncItem() :
  union_engine_(NULL),
  whiteout_(false),
  valid_graft_(false),
  graft_marker_present_(false),
  size_(-1),
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
  valid_graft_(false),
  graft_marker_present_(false),
  relative_parent_path_(relative_parent_path),
  filename_(filename),
  size_(-1),
  scratch_type_(entry_type),
  rdonly_type_(kItemUnknown)
{
  content_hash_.algorithm = shash::kAny;
  CheckGraft();
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
  // TODO(rmeusel): is this a good idea here?
  dirent.linkcount_      = this->GetUnionStat().st_nlink;

  dirent.mode_           = this->GetUnionStat().st_mode;
  dirent.uid_            = this->GetUnionStat().st_uid;
  dirent.gid_            = this->GetUnionStat().st_gid;
  dirent.size_           = size_ > -1 ? size_ : this->GetUnionStat().st_size;
  dirent.mtime_          = this->GetUnionStat().st_mtime;
  dirent.checksum_       = this->GetContentHash();

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


std::string SyncItem::GetGraftPath() const {
  return (relative_parent_path_.empty()) ?
      ".cvmfsgraft-" + filename_         :
      relative_parent_path_ + (filename_.empty() ? "" : ("/.cvmfsgraft-" + filename_));
}

void SyncItem::CheckGraft() {
  valid_graft_ = false;
  bool found_checksum = false;
  std::string checksum_type;
  std::string checksum_value;
  std::string graftfile = GetGraftPath();
  LogCvmfs(kLogFsTraversal, kLogDebug, "Checking potential graft path %s.", graftfile.c_str());
  FILE *fp = fopen(graftfile.c_str(), "r");
  if (fp == NULL) {
    if (errno != ENOENT) {
      LogCvmfs(kLogFsTraversal, kLogWarning, "Unable to open graft file (%s): %s (errno=%d)",
               graftfile.c_str(), strerror(errno), errno);
    }
    return;
  }
  graft_marker_present_ = true;
  size_t len = 0;
  ssize_t read;
  char *line = NULL;
  while (1) {
    read = getline(&line, &len, fp);
    if (read == -1) {
      if (errno == EINTR) {continue;}
      else {
        break;
      }
    }
    if (line[read-1] == '\n') {line[read-1] = '\0';}
    char *value;
    if (line == (value = strcasestr(line, "size="))) {
      value += 5;
      size_t input_len = strlen(value);
      char *endptr=NULL;
      errno = 0;
      long long mysize = strtoll(value, &endptr, 10);
      if (input_len == 0 || endptr != (value+input_len)) {
        LogCvmfs(kLogFsTraversal, kLogWarning, "size= line was passed no value.");
        continue;
      }
      if (errno) {
        LogCvmfs(kLogFsTraversal, kLogWarning, "Failed to parse value of %s to integer: %s (errno=%d)", line, strerror(errno), errno);
        continue;
      }
      size_ = mysize;
    } else if (line == (value = strcasestr(line, "checksum="))) {
      value += 9;
      shash::HexPtr hashP(value);
      if (hashP.IsValid()) {
        content_hash_ = shash::MkFromHexPtr(hashP);
        found_checksum = true;
      } else {
        LogCvmfs(kLogFsTraversal, kLogWarning, "Invalid checksum value: %s.", value);
      }
      continue;
    } else if ((strlen(line) > 0) && (line[0] != '#')) {
      LogCvmfs(kLogFsTraversal, kLogDebug, "Unknown graft attribute: %s.", line);
    }
  }
  free(line);
  if (!feof(fp)) {
    LogCvmfs(kLogFsTraversal, kLogWarning, "Unable to read from catalog marker (%s): %s (errno=%d)",
             graftfile.c_str(), strerror(errno), errno);
  }
  fclose(fp);
  valid_graft_ = (size_ > -1) && found_checksum;
  return;
}


}  // namespace publish
