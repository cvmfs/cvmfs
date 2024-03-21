/**
 * This file is part of the CernVM File System.
 *
 * This tool checks a cvmfs repository for file catalog errors.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "swissknife_check.h"

#include <inttypes.h>
#include <unistd.h>

#include <cassert>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "catalog_sql.h"
#include "compression.h"
#include "file_chunk.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "network/download.h"
#include "reflog.h"
#include "sanitizer.h"
#include "shortstring.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"

using namespace std;  // NOLINT

// for map of duplicate entries; as in kvstore.cc
static inline uint32_t hasher_any(const shash::Any &key) {
  // We'll just do the same thing as hasher_md5, since every hash is at
  // least as large.
  return *const_cast<uint32_t *>(
             reinterpret_cast<const uint32_t *>(key.digest) + 1);
}


namespace swissknife {

CommandCheck::CommandCheck()
    : check_chunks_(false)
    , no_duplicates_map_(false)
    , is_remote_(false) {
    const shash::Any hash_null;
    duplicates_map_.Init(16, hash_null, hasher_any);
}

bool CommandCheck::CompareEntries(const catalog::DirectoryEntry &a,
                                  const catalog::DirectoryEntry &b,
                                  const bool compare_names,
                                  const bool is_transition_point)
{
  typedef catalog::DirectoryEntry::Difference Difference;

  catalog::DirectoryEntry::Differences diffs = a.CompareTo(b);
  if (diffs == Difference::kIdentical) {
    return true;
  }

  // in case of a nested catalog transition point the controlling flags are
  // supposed to differ. If this is the only difference we are done...
  if (is_transition_point &&
      (diffs ^ Difference::kNestedCatalogTransitionFlags) == 0) {
    return true;
  }

  bool retval = true;
  if (compare_names) {
    if (diffs & Difference::kName) {
      LogCvmfs(kLogCvmfs, kLogStderr, "names differ: %s / %s",
               a.name().c_str(), b.name().c_str());
      retval = false;
    }
  }
  if (diffs & Difference::kLinkcount) {
    LogCvmfs(kLogCvmfs, kLogStderr, "linkcounts differ: %u / %u",
             a.linkcount(), b.linkcount());
    retval = false;
  }
  if (diffs & Difference::kHardlinkGroup) {
    LogCvmfs(kLogCvmfs, kLogStderr, "hardlink groups differ: %u / %u",
             a.hardlink_group(), b.hardlink_group());
    retval = false;
  }
  if (diffs & Difference::kSize) {
    LogCvmfs(kLogCvmfs, kLogStderr, "sizes differ: %" PRIu64 " / %" PRIu64,
             a.size(), b.size());
    retval = false;
  }
  if (diffs & Difference::kMode) {
    LogCvmfs(kLogCvmfs, kLogStderr, "modes differ: %u / %u",
             a.mode(), b.mode());
    retval = false;
  }
  if (diffs & Difference::kMtime) {
    LogCvmfs(kLogCvmfs, kLogStderr, "timestamps differ: %lu / %lu",
             a.mtime(), b.mtime());
    retval = false;
  }
  if (diffs & Difference::kChecksum) {
    LogCvmfs(kLogCvmfs, kLogStderr, "content hashes differ: %s / %s",
             a.checksum().ToString().c_str(), b.checksum().ToString().c_str());
    retval = false;
  }
  if (diffs & Difference::kSymlink) {
    LogCvmfs(kLogCvmfs, kLogStderr, "symlinks differ: %s / %s",
             a.symlink().c_str(), b.symlink().c_str());
    retval = false;
  }
  if (diffs & Difference::kExternalFileFlag) {
    LogCvmfs(kLogCvmfs, kLogStderr, "external file flag differs: %d / %d "
             "(%s / %s)", a.IsExternalFile(), b.IsExternalFile(),
             a.name().c_str(), b.name().c_str());
    retval = false;
  }
  if (diffs & Difference::kHasXattrsFlag) {
    LogCvmfs(kLogCvmfs, kLogStderr, "extended attributes differ: %d / %d "
             "(%s / %s)", a.HasXattrs(), b.HasXattrs(),
             a.name().c_str(), b.name().c_str());
    retval = false;
  }

  return retval;
}


bool CommandCheck::CompareCounters(const catalog::Counters &a,
                                   const catalog::Counters &b)
{
  const catalog::Counters::FieldsMap map_a = a.GetFieldsMap();
  const catalog::Counters::FieldsMap map_b = b.GetFieldsMap();

  bool retval = true;
  catalog::Counters::FieldsMap::const_iterator i    = map_a.begin();
  catalog::Counters::FieldsMap::const_iterator iend = map_a.end();
  for (; i != iend; ++i) {
    catalog::Counters::FieldsMap::const_iterator comp = map_b.find(i->first);
    assert(comp != map_b.end());

    if (*(i->second) != *(comp->second)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "catalog statistics mismatch: %s (expected: %" PRIu64 " / "
               "in catalog: %" PRIu64 ")",
               comp->first.c_str(), *(i->second), *(comp->second));
      retval = false;
    }
  }

  return retval;
}


/**
 * Checks for existence of a file either locally or via HTTP head
 */
bool CommandCheck::Exists(const string &file)
{
  if (!is_remote_) {
    return FileExists(file) || SymlinkExists(file);
  } else {
    const string url = repo_base_path_ + "/" + file;
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "[Exists::url] %s", url.c_str());
    download::JobInfo head(&url, false);
    return download_manager()->Fetch(&head) == download::kFailOk;
  }
}


/**
 * Copies a file from the repository into a temporary file.
 */
string CommandCheck::FetchPath(const string &path) {
  string tmp_path;
  FILE *f = CreateTempFile(temp_directory_ + "/cvmfstmp", kDefaultFileMode,
                           "w+", &tmp_path);
  assert(f != NULL);

  const string url = repo_base_path_ + "/" + path;
  if (is_remote_) {
    cvmfs::FileSink filesink(f);
    download::JobInfo download_job(&url, false, false, NULL, &filesink);
    download::Failures retval = download_manager()->Fetch(&download_job);
    if (retval != download::kFailOk) {
      PANIC(kLogStderr, "failed to read %s", url.c_str());
    }
  } else {
    bool retval = CopyPath2File(url, f);
    if (!retval) {
      PANIC(kLogStderr, "failed to read %s", url.c_str());
    }
  }

  fclose(f);
  return tmp_path;
}


/**
 * Verifies reflog checksum and looks for presence of the entry points
 * referenced in the manifest.
 */
bool CommandCheck::InspectReflog(
  const shash::Any &reflog_hash,
  manifest::Manifest *manifest)
{
  LogCvmfs(kLogCvmfs, kLogStdout, "Inspecting log of references");
  string reflog_path = FetchPath(".cvmfsreflog");
  shash::Any computed_hash(reflog_hash.algorithm);
  manifest::Reflog::HashDatabase(reflog_path, &computed_hash);
  if (computed_hash != reflog_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "The .cvmfsreflog has unexpected content hash %s (expected %s)",
             computed_hash.ToString().c_str(), reflog_hash.ToString().c_str());
    unlink(reflog_path.c_str());
    return false;
  }

  UniquePtr<manifest::Reflog> reflog(manifest::Reflog::Open(reflog_path));
  assert(reflog.IsValid());
  reflog->TakeDatabaseFileOwnership();

  if (!reflog->ContainsCatalog(manifest->catalog_hash())) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to find catalog root hash %s in .cvmfsreflog",
             manifest->catalog_hash().ToString().c_str());
    return false;
  }

  if (!reflog->ContainsCertificate(manifest->certificate())) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to find certificate hash %s in .cvmfsreflog",
             manifest->certificate().ToString().c_str());
    return false;
  }

  if (!manifest->history().IsNull() &&
      !reflog->ContainsHistory(manifest->history()))
  {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to find tag database's hash %s in .cvmfsreflog",
             manifest->history().ToString().c_str());
    return false;
  }

  if (!manifest->meta_info().IsNull() &&
      !reflog->ContainsMetainfo(manifest->meta_info()))
  {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to find meta info hash %s in .cvmfsreflog",
             manifest->meta_info().ToString().c_str());
    return false;
  }

  return true;
}


/**
 * Verifies the logical consistency of the tag database.
 */
bool CommandCheck::InspectHistory(history::History *history) {
  LogCvmfs(kLogCvmfs, kLogStdout, "Inspecting tag database");
  bool retval;
  vector<history::History::Tag> tags;
  retval = history->List(&tags);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to enumerate tags");
    return false;
  }
  vector<history::History::Branch> branches;
  retval = history->ListBranches(&branches);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to enumerate branches");
    return false;
  }

  bool result = true;

  map<string, uint64_t> initial_revisions;
  sanitizer::BranchSanitizer sanitizer;
  for (unsigned i = 0; i < branches.size(); ++i) {
    if (!sanitizer.IsValid(branches[i].branch)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid branch name: %s",
               branches[i].branch.c_str());
      result = false;
    }
    initial_revisions[branches[i].branch] = branches[i].initial_revision;
  }

  set<string> used_branches;  // all branches referenced in tag db
  // TODO(jblomer): same root hash implies same size and revision
  for (unsigned i = 0; i < tags.size(); ++i) {
    used_branches.insert(tags[i].branch);
    const map<string, uint64_t>::const_iterator iter =
      initial_revisions.find(tags[i].branch);
    if (iter == initial_revisions.end()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid branch %s in tag %s",
               tags[i].branch.c_str(), tags[i].name.c_str());
      result = false;
    } else {
      if (tags[i].revision < iter->second) {
        LogCvmfs(kLogCvmfs, kLogStderr, "invalid revision %" PRIu64
                 " of tag %s", tags[i].revision, tags[i].name.c_str());
        result = false;
      }
    }
  }

  if (used_branches.size() != branches.size()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "unused, dangling branches stored");
    result = false;
  }

  return result;
}


/**
 * Recursive catalog walk-through
 *
 * TODO(vavolkl): This method is large and does a lot of checks
 * that could be split into smaller ones.
 *
 */
bool CommandCheck::Find(const catalog::Catalog *catalog,
                        const PathString &path,
                        catalog::DeltaCounters *computed_counters,
                        set<PathString> *bind_mountpoints)
{
  catalog::DirectoryEntryList entries;
  catalog::DirectoryEntry this_directory;

  if (!catalog->LookupPath(path, &this_directory)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup %s",
             path.c_str());
    return false;
  }
  if (!catalog->ListingPath(path, &entries)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to list %s",
             path.c_str());
    return false;
  }

  uint32_t num_subdirs = 0;
  bool retval = true;
  typedef map< uint32_t, vector<catalog::DirectoryEntry> > HardlinkMap;
  HardlinkMap hardlinks;
  bool found_nested_marker = false;

  for (unsigned i = 0; i < entries.size(); ++i) {
    // for performance reasons, keep track of files already checked
    // and only run requests once per hash
    const bool entry_needs_check =
          !entries[i].checksum().IsNull() && !entries[i].IsExternalFile() &&
         // fallback cli option can force the entry to be checked
          (no_duplicates_map_ ||
             !duplicates_map_.Contains(entries[i].checksum()));
    if (entry_needs_check && !no_duplicates_map_)
        duplicates_map_.Insert(entries[i].checksum(), 1);

    PathString full_path(path);
    full_path.Append("/", 1);
    full_path.Append(entries[i].name().GetChars(),
                     entries[i].name().GetLength());
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "[path] %s [needs check] %i",
             full_path.c_str(), entry_needs_check);


    // Name must not be empty
    if (entries[i].name().IsEmpty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "empty path at %s",
               full_path.c_str());
      retval = false;
    }

    // Catalog markers should indicate nested catalogs
    if (entries[i].name() == NameString(string(".cvmfscatalog"))) {
      if (catalog->mountpoint() != path) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "found abandoned nested catalog marker at %s",
                 full_path.c_str());
        retval = false;
      }
      found_nested_marker = true;
    }

    // Check if checksum is not null
    if (entries[i].IsRegular() && !entries[i].IsChunkedFile() &&
        entries[i].checksum().IsNull())
    {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "regular file pointing to zero-hash: '%s'", full_path.c_str());
      retval = false;
    }

    // Check if the chunk is there
    if (check_chunks_ && entry_needs_check)
    {
      string chunk_path = "data/" + entries[i].checksum().MakePath();
      if (entries[i].IsDirectory())
        chunk_path += shash::kSuffixMicroCatalog;
      if (!Exists(chunk_path)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "data chunk %s (%s) missing",
                 entries[i].checksum().ToString().c_str(), full_path.c_str());
        retval = false;
      }
    }

    // Add hardlinks to counting map
    if ((entries[i].linkcount() > 1) && !entries[i].IsDirectory()) {
      if (entries[i].hardlink_group() == 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "invalid hardlink group for %s",
                 full_path.c_str());
        retval = false;
      } else {
        HardlinkMap::iterator hardlink_group =
          hardlinks.find(entries[i].hardlink_group());
        if (hardlink_group == hardlinks.end()) {
          hardlinks[entries[i].hardlink_group()];
          hardlinks[entries[i].hardlink_group()].push_back(entries[i]);
        } else {
          if (!CompareEntries(entries[i], (hardlink_group->second)[0], false)) {
            LogCvmfs(kLogCvmfs, kLogStderr, "hardlink %s doesn't match",
                     full_path.c_str());
            retval = false;
          }
          hardlink_group->second.push_back(entries[i]);
        }  // Hardlink added to map
      }  // Hardlink group > 0
    }  // Hardlink found

    // For any kind of entry, the linkcount should be > 0
    if (entries[i].linkcount() == 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Entry %s has linkcount 0.",
               entries[i].name().c_str());
      retval = false;
    }

    // Checks depending of entry type
    if (!entries[i].IsRegular()) {
      if (entries[i].IsDirectIo()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "invalid direct i/o flag found: %s",
                 full_path.c_str());
        retval = false;
      }
    }
    if (entries[i].IsDirectory()) {
      computed_counters->self.directories++;
      num_subdirs++;
      // Directory size
      // if (entries[i].size() < 4096) {
      //   LogCvmfs(kLogCvmfs, kLogStderr, "invalid file size for %s",
      //            full_path.c_str());
      //   retval = false;
      // }
      // No directory hardlinks
      if (entries[i].hardlink_group() != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "directory hardlink found at %s",
                 full_path.c_str());
        retval = false;
      }
      if (entries[i].IsNestedCatalogMountpoint() ||
          entries[i].IsBindMountpoint())
      {
        // Find transition point
        if (entries[i].IsNestedCatalogMountpoint())
          computed_counters->self.nested_catalogs++;
        shash::Any tmp;
        uint64_t tmp2;
        PathString mountpoint(full_path);
        if (!catalog->FindNested(mountpoint, &tmp, &tmp2)) {
          LogCvmfs(kLogCvmfs, kLogStderr, "nested catalog at %s not registered",
                   full_path.c_str());
          retval = false;
        }

        // check that the nested mountpoint is empty in the current catalog
        catalog::DirectoryEntryList nested_entries;
        if (catalog->ListingPath(full_path, &nested_entries) &&
            !nested_entries.empty()) {
          LogCvmfs(kLogCvmfs, kLogStderr, "non-empty nested catalog mountpoint "
                                          "at %s.",
                   full_path.c_str());
          retval = false;
        }

        if (entries[i].IsBindMountpoint()) {
          bind_mountpoints->insert(full_path);
          if (entries[i].IsNestedCatalogMountpoint()) {
            LogCvmfs(kLogCvmfs, kLogStderr,
                     "bind mountpoint and nested mountpoint mutually exclusive"
                     " at %s.", full_path.c_str());
            retval = false;
          }
        }
      } else {
        // Recurse
        if (!Find(catalog, full_path, computed_counters, bind_mountpoints))
          retval = false;
      }
    } else if (entries[i].IsLink()) {
      computed_counters->self.symlinks++;
      // No hash for symbolics links
      if (!entries[i].checksum().IsNull()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "symbolic links with hash at %s",
                 full_path.c_str());
        retval = false;
      }
      // Right size of symbolic link?
      if (entries[i].size() != entries[i].symlink().GetLength()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "wrong symbolic link size for %s; "
                 "expected %u, got %lu", full_path.c_str(),
                 entries[i].symlink().GetLength(), entries[i].size());
        retval = false;
      }
    } else if (entries[i].IsRegular()) {
      computed_counters->self.regular_files++;
      computed_counters->self.file_size += entries[i].size();
    } else if (entries[i].IsSpecial()) {
      computed_counters->self.specials++;
      // Size zero for special files
      if (entries[i].size() != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "unexpected non-zero special file size %s",
                 full_path.c_str());
        retval = false;
      }
      // No hash for special files
      if (!entries[i].checksum().IsNull()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "special file with hash at %s",
                 full_path.c_str());
        retval = false;
      }
      // No symlink
      if (entries[i].symlink().GetLength() > 0) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "special file with non-zero symlink at %s", full_path.c_str());
        retval = false;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown file type %s",
               full_path.c_str());
      retval = false;
    }

    if (entries[i].HasXattrs()) {
      computed_counters->self.xattrs++;
    }

    if (entries[i].IsExternalFile()) {
      computed_counters->self.externals++;
      computed_counters->self.external_file_size += entries[i].size();
      if (!entries[i].IsRegular()) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "only regular files can be external: %s", full_path.c_str());
        retval = false;
      }
    }

    // checking file chunk integrity
    if (entries[i].IsChunkedFile()) {
      FileChunkList chunks;
      catalog->ListPathChunks(full_path, entries[i].hash_algorithm(), &chunks);

      computed_counters->self.chunked_files++;
      computed_counters->self.chunked_file_size += entries[i].size();
      computed_counters->self.file_chunks       += chunks.size();

      // do we find file chunks for the chunked file in this catalog?
      if (chunks.size() == 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "no file chunks found for big file %s",
                 full_path.c_str());
        retval = false;
      }

      size_t aggregated_file_size = 0;
      off_t  next_offset          = 0;

      for (unsigned j = 0; j < chunks.size(); ++j) {
        FileChunk this_chunk = chunks.At(j);
        // check if the chunk boundaries fit together...
        if (next_offset != this_chunk.offset()) {
          LogCvmfs(kLogCvmfs, kLogStderr, "misaligned chunk offsets for %s",
                   full_path.c_str());
          retval = false;
        }
        next_offset = this_chunk.offset() + this_chunk.size();
        aggregated_file_size += this_chunk.size();

        // are all data chunks in the data store?
        if (check_chunks_ && !entries[i].IsExternalFile()) {
          const shash::Any &chunk_hash = this_chunk.content_hash();
          // for performance reasons, only perform the check once
          // and skip if the hash has been checked before
          bool chunk_needs_check = true;
          if (!no_duplicates_map_ && !duplicates_map_.Contains(chunk_hash)) {
            duplicates_map_.Insert(chunk_hash, 1);
          } else if (!no_duplicates_map_) {
            chunk_needs_check = false;
          }
          if (chunk_needs_check) {
           const string chunk_path = "data/" + chunk_hash.MakePath();
           if (!Exists(chunk_path)) {
              LogCvmfs(kLogCvmfs, kLogStderr, "partial data chunk %s (%s -> "
                       "offset: %ld | size: %lu) missing",
                       this_chunk.content_hash().ToStringWithSuffix().c_str(),
                       full_path.c_str(),
                       this_chunk.offset(),
                       this_chunk.size());
              retval = false;
            }
          }
        }
      }

      // is the aggregated chunk size equal to the actual file size?
      if (aggregated_file_size != entries[i].size()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "chunks of file %s produce a size "
                                        "mismatch. Calculated %zu bytes | %lu "
                                        "bytes expected",
                 full_path.c_str(),
                 aggregated_file_size,
                 entries[i].size());
        retval = false;
      }
    }
  }  // Loop through entries

  // Check if nested catalog marker has been found
  if (!path.IsEmpty() && (path == catalog->mountpoint()) &&
      !found_nested_marker)
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "nested catalog without marker at %s",
             path.c_str());
    retval = false;
  }

  // Check directory linkcount
  if (this_directory.linkcount() != num_subdirs + 2) {
    LogCvmfs(kLogCvmfs, kLogStderr, "wrong linkcount for %s; "
             "expected %u, got %u",
             path.c_str(), num_subdirs + 2, this_directory.linkcount());
    retval = false;
  }

  // Check hardlink linkcounts
  for (HardlinkMap::const_iterator i = hardlinks.begin(),
       iEnd = hardlinks.end(); i != iEnd; ++i)
  {
    if (i->second[0].linkcount() != i->second.size()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "hardlink linkcount wrong for %s, "
               "expected %lu, got %u",
               (path.ToString() + "/" + i->second[0].name().ToString()).c_str(),
               i->second.size(), i->second[0].linkcount());
      retval = false;
    }
  }

  return retval;
}


string CommandCheck::DownloadPiece(const shash::Any catalog_hash) {
  string source = "data/" + catalog_hash.MakePath();
  const string dest = temp_directory_ + "/" + catalog_hash.ToString();
  const string url = repo_base_path_ + "/" + source;

  cvmfs::PathSink pathsink(dest);
  download::JobInfo download_catalog(&url, true, false, &catalog_hash,
                                     &pathsink);
  download::Failures retval = download_manager()->Fetch(&download_catalog);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download object %s (%d)",
             catalog_hash.ToString().c_str(), retval);
    return "";
  }

  return dest;
}


string CommandCheck::DecompressPiece(const shash::Any catalog_hash) {
  string source = "data/" + catalog_hash.MakePath();
  const string dest = temp_directory_ + "/" + catalog_hash.ToString();
  if (!zlib::DecompressPath2Path(source, dest))
    return "";

  return dest;
}


catalog::Catalog* CommandCheck::FetchCatalog(const string      &path,
                                             const shash::Any  &catalog_hash,
                                             const uint64_t     catalog_size) {
  string tmp_file;
  if (!is_remote_)
    tmp_file = DecompressPiece(catalog_hash);
  else
    tmp_file = DownloadPiece(catalog_hash);

  if (tmp_file == "") {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load catalog %s",
             catalog_hash.ToString().c_str());
    return NULL;
  }

  catalog::Catalog *catalog =
                   catalog::Catalog::AttachFreely(path, tmp_file, catalog_hash);
  int64_t catalog_file_size = GetFileSize(tmp_file);
  assert(catalog_file_size > 0);
  unlink(tmp_file.c_str());

  if ((catalog_size > 0) && (uint64_t(catalog_file_size) != catalog_size)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "catalog file size mismatch, "
             "expected %" PRIu64 ", got %" PRIu64,
             catalog_size, catalog_file_size);
    delete catalog;
    return NULL;
  }

  return catalog;
}


bool CommandCheck::FindSubtreeRootCatalog(const string &subtree_path,
                                          shash::Any   *root_hash,
                                          uint64_t     *root_size) {
  catalog::Catalog *current_catalog = FetchCatalog("", *root_hash);
  if (current_catalog == NULL) {
    return false;
  }

  typedef vector<string> Tokens;
  const Tokens path_tokens = SplitString(subtree_path, '/');

  string current_path = "";

  Tokens::const_iterator i    = path_tokens.begin();
  Tokens::const_iterator iend = path_tokens.end();
  for (; i != iend; ++i) {
    if (i->empty()) {
      continue;
    }

    current_path += "/" + *i;
    if (current_catalog->FindNested(PathString(current_path),
                                    root_hash,
                                    root_size)) {
      delete current_catalog;

      if (current_path.length() < subtree_path.length()) {
        current_catalog = FetchCatalog(current_path, *root_hash);
        if (current_catalog == NULL) {
          break;
        }
      } else {
        return true;
      }
    }
  }
  return false;
}


/**
 * Recursion on nested catalog level.  No ownership of computed_counters.
 */
bool CommandCheck::InspectTree(const string                  &path,
                               const shash::Any              &catalog_hash,
                               const uint64_t                 catalog_size,
                               const bool                     is_nested_catalog,
                               const catalog::DirectoryEntry *transition_point,
                               catalog::DeltaCounters        *computed_counters)
{
  LogCvmfs(kLogCvmfs, kLogStdout | kLogInform, "[inspecting catalog] %s at %s",
           catalog_hash.ToString().c_str(), path == "" ? "/" : path.c_str());

  const catalog::Catalog *catalog = FetchCatalog(path,
                                                 catalog_hash,
                                                 catalog_size);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open catalog %s",
             catalog_hash.ToString().c_str());
    return false;
  }

  int retval = true;

  if (catalog->root_prefix() != PathString(path.data(), path.length())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "root prefix mismatch; "
             "expected %s, got %s",
             path.c_str(), catalog->root_prefix().c_str());
    retval = false;
  }

  // Check transition point
  catalog::DirectoryEntry root_entry;
  if (!catalog->LookupPath(catalog->root_prefix(), &root_entry)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup root entry (%s)",
             path.c_str());
    retval = false;
  }
  if (!root_entry.IsDirectory()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "root entry not a directory (%s)",
             path.c_str());
    retval = false;
  }
  if (is_nested_catalog) {
    if (transition_point != NULL &&
        !CompareEntries(*transition_point, root_entry, true, true)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "transition point and root entry differ (%s)", path.c_str());
      retval = false;
    }
    if (!root_entry.IsNestedCatalogRoot()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "nested catalog root expected but not found (%s)", path.c_str());
      retval = false;
    }
  } else {
    if (root_entry.IsNestedCatalogRoot()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "nested catalog root found but not expected (%s)", path.c_str());
      retval = false;
    }
  }

  // Traverse the catalog
  set<PathString> bind_mountpoints;
  if (!Find(catalog, PathString(path.data(), path.length()),
            computed_counters, &bind_mountpoints))
  {
    retval = false;
  }

  // Check number of entries
  if (root_entry.HasXattrs())
    computed_counters->self.xattrs++;
  const uint64_t num_found_entries =
    1 +
    computed_counters->self.regular_files +
    computed_counters->self.symlinks +
    computed_counters->self.specials +
    computed_counters->self.directories;
  if (num_found_entries != catalog->GetNumEntries()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "dangling entries in catalog, "
             "expected %" PRIu64 ", got %" PRIu64,
             catalog->GetNumEntries(), num_found_entries);
    retval = false;
  }

  // Recurse into nested catalogs
  const catalog::Catalog::NestedCatalogList &nested_catalogs =
    catalog->ListNestedCatalogs();
  const catalog::Catalog::NestedCatalogList own_nested_catalogs =
    catalog->ListOwnNestedCatalogs();
  if (own_nested_catalogs.size() !=
      static_cast<uint64_t>(computed_counters->self.nested_catalogs))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "number of nested catalogs does not match;"
             " expected %lu, got %lu", computed_counters->self.nested_catalogs,
             own_nested_catalogs.size());
    retval = false;
  }
  set<PathString> nested_catalog_paths;
  for (catalog::Catalog::NestedCatalogList::const_iterator i =
       nested_catalogs.begin(), iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    nested_catalog_paths.insert(i->mountpoint);
  }
  if (nested_catalog_paths.size() != nested_catalogs.size()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "duplicates among nested catalogs and bind mountpoints");
    retval = false;
  }

  for (catalog::Catalog::NestedCatalogList::const_iterator i =
       nested_catalogs.begin(), iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    if (bind_mountpoints.find(i->mountpoint) != bind_mountpoints.end()) {
      catalog::DirectoryEntry bind_mountpoint;
      PathString mountpoint("/" + i->mountpoint.ToString().substr(1));
      if (!catalog->LookupPath(mountpoint, &bind_mountpoint)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup bind mountpoint %s",
                 mountpoint.c_str());
        retval = false;
      }
      LogCvmfs(kLogCvmfs, kLogDebug, "skipping bind mountpoint %s",
               i->mountpoint.c_str());
      continue;
    }
    catalog::DirectoryEntry nested_transition_point;
    if (!catalog->LookupPath(i->mountpoint, &nested_transition_point)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup transition point %s",
               i->mountpoint.c_str());
      retval = false;
    } else {
      catalog::DeltaCounters nested_counters;
      const bool is_nested = true;
      if (!InspectTree(i->mountpoint.ToString(), i->hash, i->size, is_nested,
                       &nested_transition_point, &nested_counters))
        retval = false;
      nested_counters.PopulateToParent(computed_counters);
    }
  }

  // Check statistics counters
  // Additionally account for root directory
  computed_counters->self.directories++;
  catalog::Counters compare_counters;
  compare_counters.ApplyDelta(*computed_counters);
  const catalog::Counters stored_counters = catalog->GetCounters();
  if (!CompareCounters(compare_counters, stored_counters)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "statistics counter mismatch [%s]",
             catalog_hash.ToString().c_str());
    retval = false;
  }

  delete catalog;
  return retval;
}


int CommandCheck::Main(const swissknife::ArgumentList &args) {
  string tag_name;
  string subtree_path = "";
  string pubkey_path = "";
  string repo_name = "";
  string reflog_chksum_path = "";

  temp_directory_ = (args.find('t') != args.end()) ? *args.find('t')->second
                                                   : "/tmp";
  if (args.find('n') != args.end())
    tag_name = *args.find('n')->second;
  if (args.find('c') != args.end())
    check_chunks_ = true;
  if (args.find('d') != args.end())
    no_duplicates_map_ = true;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      kLogLevel0 << String2Uint64(*args.find('l')->second);
    if (log_level > kLogNone) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid log level");
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  if (args.find('k') != args.end())
    pubkey_path = *args.find('k')->second;
  if (DirectoryExists(pubkey_path))
    pubkey_path = JoinStrings(FindFilesBySuffix(pubkey_path, ".pub"), ":");
  if (args.find('N') != args.end())
    repo_name = *args.find('N')->second;

  repo_base_path_ = MakeCanonicalPath(*args.find('r')->second);
  if (args.find('s') != args.end())
    subtree_path = MakeCanonicalPath(*args.find('s')->second);
  if (args.find('R') != args.end())
    reflog_chksum_path = *args.find('R')->second;

  // Repository can be HTTP address or on local file system
  is_remote_ = IsHttpUrl(repo_base_path_);

  // initialize the (swissknife global) download and signature managers
  if (is_remote_) {
    const bool follow_redirects = (args.count('L') > 0);
    const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
    if (!this->InitDownloadManager(follow_redirects, proxy)) {
      return 1;
    }

    if (pubkey_path.empty() || repo_name.empty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "please provide pubkey and repo name for "
                                      "remote repositories");
      return 1;
    }

    if (!this->InitSignatureManager(pubkey_path)) {
      return 1;
    }
  }

  // Load Manifest
  UniquePtr<manifest::Manifest> manifest;
  bool successful = true;

  if (is_remote_) {
    manifest = FetchRemoteManifest(repo_base_path_, repo_name);
  } else {
    if (chdir(repo_base_path_.c_str()) != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to switch to directory %s",
               repo_base_path_.c_str());
      return 1;
    }
    manifest = OpenLocalManifest(".cvmfspublished");
  }

  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return 1;
  }

  // Check meta-info object
  if (!manifest->meta_info().IsNull()) {
    string tmp_file;
    if (!is_remote_)
      tmp_file = DecompressPiece(manifest->meta_info());
    else
      tmp_file = DownloadPiece(manifest->meta_info());
    if (tmp_file == "") {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository metainfo %s",
               manifest->meta_info().ToString().c_str());
      return 1;
    }
    unlink(tmp_file.c_str());
  }

  shash::Any reflog_hash;
  if (!reflog_chksum_path.empty()) {
    if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read reflog checksum file");
      return 1;
    }
  } else {
    reflog_hash = manifest->reflog_hash();
  }

  if (Exists(".cvmfsreflog")) {
    if (reflog_hash.IsNull()) {
      // If there is a reflog, we want to check it
      LogCvmfs(kLogCvmfs, kLogStderr,
               ".cvmfsreflog present but no checksum provided, aborting");
      return 1;
    }
    bool retval = InspectReflog(reflog_hash, manifest.weak_ref());
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to verify reflog");
      return 1;
    }
  } else {
    if (!reflog_hash.IsNull()) {
      // There is a checksum but no reflog; possibly the checksum is for the
      // from the manifest for the stratum 0 reflog
      if (!reflog_chksum_path.empty()) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "local reflog checksum set but reflog itself is missing, "
                 "aborting");
        return 1;
      }
    }
  }

  // Load history
  UniquePtr<history::History> tag_db;
  if (!manifest->history().IsNull()) {
    string tmp_file;
    if (!is_remote_)
      tmp_file = DecompressPiece(manifest->history());
    else
      tmp_file = DownloadPiece(manifest->history());
    if (tmp_file == "") {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load history database %s",
               manifest->history().ToString().c_str());
      return 1;
    }
    tag_db = history::SqliteHistory::Open(tmp_file);
    if (!tag_db.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open history database %s",
               manifest->history().ToString().c_str());
      return 1;
    }
    tag_db->TakeDatabaseFileOwnership();
    successful = InspectHistory(tag_db.weak_ref()) && successful;
  }

  if (manifest->has_alt_catalog_path()) {
    if (!Exists(manifest->certificate().MakeAlternativePath())) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to find alternative certificate link %s",
               manifest->certificate().MakeAlternativePath().c_str());
      return 1;
    }
    if (!Exists(manifest->catalog_hash().MakeAlternativePath())) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to find alternative catalog link %s",
               manifest->catalog_hash().MakeAlternativePath().c_str());
      return 1;
    }
  }

  shash::Any root_hash = manifest->catalog_hash();
  uint64_t root_size = manifest->catalog_size();
  if (tag_name != "") {
    if (!tag_db.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "no history");
      return 1;
    }
    history::History::Tag tag;
    const bool retval = tag_db->GetByName(tag_name, &tag);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "no such tag: %s", tag_name.c_str());
      return 1;
    }
    root_hash = tag.root_hash;
    root_size = tag.size;
    LogCvmfs(kLogCvmfs, kLogStdout, "Inspecting repository tag %s",
             tag_name.c_str());
  }

  const bool is_nested_catalog = (!subtree_path.empty());
  if (is_nested_catalog && !FindSubtreeRootCatalog( subtree_path,
                                                   &root_hash,
                                                   &root_size)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "cannot find nested catalog at %s",
             subtree_path.c_str());
    return 1;
  }


  catalog::DeltaCounters computed_counters;
  successful = InspectTree(subtree_path,
                           root_hash,
                           root_size,
                           is_nested_catalog,
                           NULL,
                           &computed_counters) && successful;

  if (!successful) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CATALOG PROBLEMS OR OTHER ERRORS FOUND");
    return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "no problems found");
  return 0;
}

}  // namespace swissknife
