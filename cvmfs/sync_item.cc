/**
 * This file is part of the CernVM file system
 */

#include "sync_item.h"

#include <cerrno>
#include <vector>

#include "sync_mediator.h"
#include "sync_union.h"

using namespace std;  // NOLINT

namespace publish {


SyncItem::SyncItem() :
  union_engine_(NULL),
  whiteout_(false),
  opaque_(false),
  masked_hardlink_(false),
  valid_graft_(false),
  graft_marker_present_(false),
  external_data_(false),
  graft_chunklist_(NULL),
  graft_size_(-1),
  scratch_type_(static_cast<SyncItemType>(0)),
  rdonly_type_(static_cast<SyncItemType>(0)),
  compression_algorithm_(zlib::kZlibDefault) {}

SyncItem::SyncItem(const string       &relative_parent_path,
                   const string       &filename,
                   const SyncUnion    *union_engine,
                   const SyncItemType  entry_type) :
  union_engine_(union_engine),
  whiteout_(false),
  opaque_(false),
  masked_hardlink_(false),
  valid_graft_(false),
  graft_marker_present_(false),
  external_data_(false),
  relative_parent_path_(relative_parent_path),
  filename_(filename),
  graft_chunklist_(NULL),
  graft_size_(-1),
  scratch_type_(entry_type),
  rdonly_type_(kItemUnknown),
  compression_algorithm_(zlib::kZlibDefault)
{
  content_hash_.algorithm = shash::kAny;
  // Note: graft marker for non-regular files are silently ignored
  if (IsRegularFile()) {CheckGraft();}
}


SyncItem::~SyncItem() {
  delete graft_chunklist_;
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

  const SyncItemType deleted_type = (rdonly_stat_.error_code == 0)
                                        ? GetRdOnlyFiletype()
                                        : kItemUnknown;

  rdonly_type_  = deleted_type;
  scratch_type_ = deleted_type;

  if (deleted_type == kItemUnknown) {
    // Marking a SyncItem as 'whiteout' but no file to be removed found: This
    // should not happen (actually AUFS prevents users from creating whiteouts)
    // but can be provoked through an AUFS 'bug' (see test 593 or CVM-880).
    // --> Warn the user, continue with kItemUnknown and cross your fingers!
    PrintWarning("'" + GetRelativePath() + "' should be deleted, but was not "
                 "found in repository.");
  }
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
  dirent.size_           = graft_size_ > -1 ? graft_size_ :
                           this->GetUnionStat().st_size;
  dirent.mtime_          = this->GetUnionStat().st_mtime;
  dirent.checksum_       = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

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


std::string SyncItem::GetGraftMarkerPath() const {
  return union_engine_->scratch_path() + "/" +
      ((relative_parent_path_.empty()) ?
        ".cvmfsgraft-" + filename_ :
        relative_parent_path_ + (filename_.empty() ? "" :
          ("/.cvmfsgraft-" + filename_)));
}

void SyncItem::CheckGraft() {
  valid_graft_ = false;
  bool found_checksum = false;
  std::string checksum_type;
  std::string checksum_value;
  std::string graftfile = GetGraftMarkerPath();
  LogCvmfs(kLogFsTraversal, kLogDebug, "Checking potential graft path %s.",
           graftfile.c_str());
  FILE *fp = fopen(graftfile.c_str(), "r");
  if (fp == NULL) {
    if (errno != ENOENT) {
      LogCvmfs(kLogFsTraversal, kLogWarning, "Unable to open graft file "
               "(%s): %s (errno=%d)",
               graftfile.c_str(), strerror(errno), errno);
    }
    return;
  }
  graft_marker_present_ = true;
  valid_graft_ = true;
  std::string line;
  std::vector<std::string> contents;

  std::vector<off_t> chunk_offsets;
  std::vector<shash::Any> chunk_checksums;

  while (GetLineFile(fp, &line)) {
    std::string trimmed_line = Trim(line);

    if (!trimmed_line.size()) {continue;}
    if (trimmed_line[0] == '#') {continue;}

    std::vector<std::string> info = SplitString(trimmed_line, '=', 2);

    if (info.size() != 2) {
      LogCvmfs(kLogFsTraversal, kLogWarning, "Invalid line in graft file: %s",
               trimmed_line.c_str());
    }
    info[0] = Trim(info[0]);
    info[1] = Trim(info[1]);
    if (info[0] == "size") {
      uint64_t tmp_size;
      if (!String2Uint64Parse(info[1], &tmp_size)) {
        LogCvmfs(kLogFsTraversal, kLogWarning, "Failed to parse value of %s "
                 "to integer: %s (errno=%d)", trimmed_line.c_str(),
                 strerror(errno), errno);
        continue;
      }
      graft_size_ = tmp_size;
    } else if (info[0] == "checksum") {
      std::string hash_str = info[1];
      shash::HexPtr hashP(hash_str);
      if (hashP.IsValid()) {
        content_hash_ = shash::MkFromHexPtr(hashP);
        found_checksum = true;
      } else {
        LogCvmfs(kLogFsTraversal, kLogWarning, "Invalid checksum value: %s.",
                 info[1].c_str());
      }
      continue;
    } else if (info[0] == "chunk_offsets") {
      std::vector<std::string> offsets = SplitString(info[1], ',');
      for (std::vector<std::string>::const_iterator it = offsets.begin();
           it != offsets.end(); it++)
      {
        uint64_t val;
        if (!String2Uint64Parse(*it, &val)) {
          valid_graft_ = false;
          LogCvmfs(kLogFsTraversal, kLogWarning, "Invalid chunk offset: %s.",
                   it->c_str());
          break;
        }
        chunk_offsets.push_back(val);
      }
    } else if (info[0] == "chunk_checksums") {
      std::vector<std::string> csums = SplitString(info[1], ',');
      for (std::vector<std::string>::const_iterator it = csums.begin();
           it != csums.end(); it++)
      {
        shash::HexPtr hashP(*it);
        if (hashP.IsValid()) {
          chunk_checksums.push_back(shash::MkFromHexPtr(hashP));
        } else {
          LogCvmfs(kLogFsTraversal, kLogWarning, "Invalid chunk checksum "
                 "value: %s.", it->c_str());
          valid_graft_ = false;
          break;
        }
      }
    }
  }
  if (!feof(fp)) {
    LogCvmfs(kLogFsTraversal, kLogWarning, "Unable to read from catalog "
             "marker (%s): %s (errno=%d)",
             graftfile.c_str(), strerror(errno), errno);
  }
  fclose(fp);
  valid_graft_ = valid_graft_ && (graft_size_ > -1) && found_checksum
                 && (chunk_checksums.size() == chunk_offsets.size());

  if (!valid_graft_ || chunk_offsets.empty())
    return;

  // Parse chunks
  graft_chunklist_ = new FileChunkList(chunk_offsets.size());
  off_t last_offset = chunk_offsets[0];
  if (last_offset != 0) {
    LogCvmfs(kLogFsTraversal, kLogWarning, "First chunk offset must be 0"
             " (in graft marker %s).", graftfile.c_str());
    valid_graft_ = false;
  }
  for (unsigned idx = 1; idx < chunk_offsets.size(); idx++) {
    off_t cur_offset = chunk_offsets[idx];
    if (last_offset >= cur_offset) {
      LogCvmfs(kLogFsTraversal, kLogWarning, "Chunk offsets must be sorted "
               "in strictly increasing order (in graft marker %s).",
               graftfile.c_str());
      valid_graft_ = false;
      break;
    }
    size_t cur_size = cur_offset - last_offset;
    graft_chunklist_->PushBack(FileChunk(chunk_checksums[idx - 1],
                                         last_offset,
                                         cur_size));
    last_offset = cur_offset;
  }
  if (graft_size_ <= last_offset) {
    LogCvmfs(kLogFsTraversal, kLogWarning, "Last offset must be strictly "
             "less than total file size (in graft marker %s).",
             graftfile.c_str());
    valid_graft_ = false;
  }
  graft_chunklist_->PushBack(FileChunk(chunk_checksums.back(),
                                        last_offset,
                                        graft_size_ - last_offset));
}

}  // namespace publish
