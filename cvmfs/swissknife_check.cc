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

#include <map>
#include <queue>
#include <string>
#include <vector>

#include "catalog_sql.h"
#include "compression.h"
#include "download.h"
#include "file_chunk.h"
#include "history_sqlite.h"
#include "logging.h"
#include "manifest.h"
#include "shortstring.h"
#include "util.h"

using namespace std;  // NOLINT

namespace swissknife {

namespace {
bool check_chunks;
std::string *remote_repository;
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
    LogCvmfs(kLogCvmfs, kLogStderr, "linkcounts differ: %lu / %lu",
             a.linkcount(), b.linkcount());
    retval = false;
  }
  if (diffs & Difference::kHardlinkGroup) {
    LogCvmfs(kLogCvmfs, kLogStderr, "hardlink groups differ: %lu / %lu",
             a.hardlink_group(), b.hardlink_group());
    retval = false;
  }
  if (diffs & Difference::kSize) {
    LogCvmfs(kLogCvmfs, kLogStderr, "sizes differ: %"PRIu64" / %"PRIu64,
             a.size(), b.size());
    retval = false;
  }
  if (diffs & Difference::kMode) {
    LogCvmfs(kLogCvmfs, kLogStderr, "modes differ: %lu / %lu",
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
               "catalog statistics mismatch: %s (expected: %"PRIu64" / "
               "in catalog: %"PRIu64")",
               comp->first.c_str(), *(i->second), *(comp->second));
      retval = false;
    }
  }

  return retval;
}


/**
 * Checks for existance of a file either locally or via HTTP head
 */
bool CommandCheck::Exists(const string &file)
{
  if (remote_repository == NULL) {
    return FileExists(file);
  } else {
    const string url = *remote_repository + "/" + file;
    download::JobInfo head(&url, false);
    return g_download_manager->Fetch(&head) == download::kFailOk;
  }
}


/**
 * Recursive catalog walk-through
 */
bool CommandCheck::Find(const catalog::Catalog *catalog,
                        const PathString &path,
                        catalog::DeltaCounters *computed_counters)
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
    PathString full_path(path);
    full_path.Append("/", 1);
    full_path.Append(entries[i].name().GetChars(),
                     entries[i].name().GetLength());
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "[path] %s",
             full_path.c_str());

    // Name must not be empty
    if (entries[i].name().IsEmpty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "empty path at %s",
               full_path.c_str());
      retval = false;
    }

    // Catalog markers should indicate nested catalogs
    if (entries[i].name() == NameString(string(".cvmfscatalog"))) {
      if (catalog->path() != path) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "found abandoned nested catalog marker at %s",
                 full_path.c_str());
        retval = false;
      }
      found_nested_marker = true;
    }

    // Check if checksum is not null
    if (entries[i].IsRegular() && entries[i].checksum().IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "regular file pointing to zero-hash: '%s'", full_path.c_str());
      retval = false;
    }

    // Check if the chunk is there
    if (!entries[i].checksum().IsNull() && check_chunks) {
      string chunk_path = "data" +
                          entries[i].checksum().MakePathExplicit(1, 2);
      if (entries[i].IsDirectory())
        chunk_path += "L";
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

    // Checks depending of entry type
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
      if (entries[i].IsNestedCatalogMountpoint()) {
        // Find transition point
        computed_counters->self.nested_catalogs++;
        shash::Any tmp;
        uint64_t tmp2;
        if (!catalog->FindNested(full_path, &tmp, &tmp2)) {
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
      } else {
        // Recurse
        if (!Find(catalog, full_path, computed_counters))
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
        LogCvmfs(kLogCvmfs, kLogStderr, "wrong synbolic link size for %s; ",
                 "expected %s, got %s", full_path.c_str(),
                 entries[i].symlink().GetLength(), entries[i].size());
        retval = false;
      }
    } else if (entries[i].IsRegular()) {
      computed_counters->self.regular_files++;
      computed_counters->self.file_size += entries[i].size();
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown file type %s",
               full_path.c_str());
      retval = false;
    }

    if (entries[i].HasXattrs()) {
      computed_counters->self.xattrs++;
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
        if (check_chunks) {
          const shash::Any &chunk_hash = this_chunk.content_hash();
          const string chunk_path = "data"                            +
                                    chunk_hash.MakePathExplicit(1, 2) +
                                    shash::kSuffixPartial;
          if (!Exists(chunk_path)) {
            const std::string chunk_name =
                   this_chunk.content_hash().ToString() + shash::kSuffixPartial;
            LogCvmfs(kLogCvmfs, kLogStderr, "partial data chunk %s (%s -> "
                                            "offset: %d | size: %d) missing",
                     chunk_name.c_str(),
                     full_path.c_str(),
                     this_chunk.offset(),
                     this_chunk.size());
            retval = false;
          }
        }
      }

      // is the aggregated chunk size equal to the actual file size?
      if (aggregated_file_size != entries[i].size()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "chunks of file %s produce a size "
                                        "mismatch. Calculated %d bytes | %d "
                                        "bytes expected",
                 full_path.c_str(),
                 aggregated_file_size,
                 entries[i].size());
        retval = false;
      }
    }
  }  // Loop through entries

  // Check if nested catalog marker has been found
  if (!path.IsEmpty() && (path == catalog->path()) && !found_nested_marker) {
    LogCvmfs(kLogCvmfs, kLogStderr, "nested catalog without marker at %s",
             path.c_str());
    retval = false;
  }

  // Check directory linkcount
  if (this_directory.linkcount() != num_subdirs + 2) {
    LogCvmfs(kLogCvmfs, kLogStderr, "wrong linkcount for %s; "
             "expected %lu, got %lu",
             path.c_str(), num_subdirs + 2, this_directory.linkcount());
    retval = false;
  }

  // Check hardlink linkcounts
  for (HardlinkMap::const_iterator i = hardlinks.begin(),
       iEnd = hardlinks.end(); i != iEnd; ++i)
  {
    if (i->second[0].linkcount() != i->second.size()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "hardlink linkcount wrong for %s, "
               "expected %lu, got %lu",
               (path.ToString() + "/" + i->second[0].name().ToString()).c_str(),
               i->second.size(), i->second[0].linkcount());
      retval = false;
    }
  }

  return retval;
}


string CommandCheck::DownloadPiece(const shash::Any catalog_hash,
                                   const char suffix)
{
  string source = "data" + catalog_hash.MakePathExplicit(1, 2);
  source.push_back(suffix);
  const string dest = "/tmp/" + catalog_hash.ToString();
  const string url = *remote_repository + "/" + source;
  download::JobInfo download_catalog(&url, true, false, &dest, &catalog_hash);
  download::Failures retval = g_download_manager->Fetch(&download_catalog);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed to download catalog %s (%d)",
             catalog_hash.ToString().c_str(), retval);
    return "";
  }

  return dest;
}


string CommandCheck::DecompressPiece(const shash::Any catalog_hash,
                                     const char suffix)
{
  string source = "data" + catalog_hash.MakePathExplicit(1, 2);
  source.push_back(suffix);
  const string dest = "/tmp/" + catalog_hash.ToString();
  if (!zlib::DecompressPath2Path(source, dest))
    return "";

  return dest;
}


/**
 * Recursion on nested catalog level.  No ownership of computed_counters.
 */
bool CommandCheck::InspectTree(const string &path,
                               const shash::Any &catalog_hash,
                               const uint64_t catalog_size,
                               const catalog::DirectoryEntry *transition_point,
                               catalog::DeltaCounters *computed_counters)
{
  LogCvmfs(kLogCvmfs, kLogStdout, "[inspecting catalog] %s at %s",
           catalog_hash.ToString().c_str(), path == "" ? "/" : path.c_str());

  string tmp_file;
  if (remote_repository == NULL)
    tmp_file = DecompressPiece(catalog_hash, 'C');
  else
    tmp_file = DownloadPiece(catalog_hash, 'C');
  if (tmp_file == "") {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed to load catalog %s",
             catalog_hash.ToString().c_str());
    return false;
  }

  int64_t catalog_file_size = GetFileSize(tmp_file);
  assert(catalog_file_size > 0);
  const catalog::Catalog *catalog =
    catalog::Catalog::AttachFreely(path, tmp_file, catalog_hash);
  unlink(tmp_file.c_str());
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed to open catalog %s",
             catalog_hash.ToString().c_str());
    return false;
  }

  int retval = true;

  if ((catalog_size > 0) && (uint64_t(catalog_file_size) != catalog_size)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "catalog file size mismatch, "
             "expected %"PRIu64", got %"PRIu64,
             catalog_size, catalog_file_size);
    retval = false;
  }

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
  if (transition_point != NULL) {
    if (!CompareEntries(*transition_point, root_entry, true, true)) {
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
  if (!Find(catalog, PathString(path.data(), path.length()), computed_counters))
  {
    retval = false;
  }

  // Check number of entries
  const uint64_t num_found_entries = 1 + computed_counters->self.regular_files +
    computed_counters->self.symlinks + computed_counters->self.directories;
  if (num_found_entries != catalog->GetNumEntries()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "dangling entries in catalog, "
             "expected %"PRIu64", got %"PRIu64,
             catalog->GetNumEntries(), num_found_entries);
    retval = false;
  }

  // Recurse into nested catalogs
  const catalog::Catalog::NestedCatalogList &nested_catalogs =
    catalog->ListNestedCatalogs();
  if (nested_catalogs.size() !=
      static_cast<uint64_t>(computed_counters->self.nested_catalogs))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "number of nested catalogs does not match;"
             " expected %lu, got %lu", computed_counters->self.nested_catalogs,
             nested_catalogs.size());
    retval = false;
  }
  for (catalog::Catalog::NestedCatalogList::const_iterator i =
       nested_catalogs.begin(), iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    catalog::DirectoryEntry nested_transition_point;
    if (!catalog->LookupPath(i->path, &nested_transition_point)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup transition point %s",
               i->path.c_str());
      retval = false;
    } else {
      catalog::DeltaCounters nested_counters;
      if (!InspectTree(i->path.ToString(), i->hash, i->size,
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
  check_chunks = false;
  if (args.find('t') != args.end())
    tag_name = *args.find('t')->second;
  if (args.find('c') != args.end())
    check_chunks = true;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  const string repository = MakeCanonicalPath(*args.find('r')->second);

  // Repository can be HTTP address or on local file system
  if (repository.substr(0, 7) == "http://") {
    remote_repository = new string(repository);
    g_download_manager->Init(1, true, g_statistics);
  } else {
    remote_repository = NULL;
  }

  // Load Manifest
  // TODO(jblomer): Do this using Manifest::Fetch() in the future
  manifest::Manifest *manifest = NULL;
  if (remote_repository == NULL) {
    if (chdir(repository.c_str()) != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to switch to directory %s",
               repository.c_str());
      return 1;
    }
    manifest = manifest::Manifest::LoadFile(".cvmfspublished");
  } else {
    const string url = repository + "/.cvmfspublished";
    download::JobInfo download_manifest(&url, false, false, NULL);
    download::Failures retval = g_download_manager->Fetch(&download_manifest);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download manifest (%d - %s)",
               retval, download::Code2Ascii(retval));
      return 1;
    }
    char *buffer = download_manifest.destination_mem.data;
    const unsigned length = download_manifest.destination_mem.size;
    manifest = manifest::Manifest::LoadMem(
      reinterpret_cast<const unsigned char *>(buffer), length);
    free(download_manifest.destination_mem.data);
  }

  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return 1;
  }

  // Validate Manifest
  const string certificate_path =
    "data" + manifest->certificate().MakePathExplicit(1, 2) + "X";
  if (!Exists(certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to find certificate (%s)",
             certificate_path.c_str());
    delete manifest;
    return 1;
  }

  shash::Any root_hash = manifest->catalog_hash();
  uint64_t root_size = manifest->catalog_size();
  if (tag_name != "") {
    if (manifest->history().IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "no history");
      delete manifest;
      return 1;
    }
    string tmp_file;
    if (remote_repository == NULL)
      tmp_file = DecompressPiece(manifest->history(), 'H');
    else
      tmp_file = DownloadPiece(manifest->history(), 'H');
    if (tmp_file == "") {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load history database %s",
               manifest->history().ToString().c_str());
      delete manifest;
      return 1;
    }
    history::History *tag_db = history::SqliteHistory::Open(tmp_file);
    if (NULL == tag_db) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to open history database %s at %s",
               manifest->history().ToString().c_str(), tmp_file.c_str());
      delete manifest;
      return 1;
    }
    history::History::Tag tag;
    const bool retval = tag_db->GetByName(tag_name, &tag);
    delete tag_db;
    unlink(tmp_file.c_str());
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStdout, "no such tag: %s", tag_name.c_str());
      unlink(tmp_file.c_str());
      delete manifest;
      return 1;
    }
    root_hash = tag.root_hash;
    root_size = tag.size;
    LogCvmfs(kLogCvmfs, kLogStdout, "Inspecting repository tag %s",
             tag_name.c_str());
  }

  catalog::DeltaCounters computed_counters;
  bool retval = InspectTree("", root_hash, root_size, NULL, &computed_counters);

  delete manifest;
  return retval ? 0 : 1;
}

}  // namespace swissknife
