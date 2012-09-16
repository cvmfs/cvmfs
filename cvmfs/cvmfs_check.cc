/**
 * This file is part of the CernVM File System.
 *
 * This tool checks a cvmfs repository for file catalog errors.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"

#include <unistd.h>
#include <inttypes.h>

#include <string>
#include <queue>
#include <vector>
#include <map>

#include "logging.h"
#include "manifest.h"
#include "util.h"
#include "hash.h"
#include "catalog.h"
#include "compression.h"
#include "shortstring.h"

using namespace std;  // NOLINT


bool check_chunks;

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM File System repository sanity checker, version %s\n\n"
    "This tool checks the consisteny of the file catalogs of a cvmfs repository."
    "\n\n"
    "Usage: cvmfs_check [options] <repository directory>\n"
    "Options:\n"
    "  -l  log level (0-4, default: 2)>\n"
    "  -c  check availability of data chunks\n",
    VERSION);
}


static bool CompareEntries(const catalog::DirectoryEntry &a,
                           const catalog::DirectoryEntry &b,
                           const bool compare_names)
{
  bool retval = true;
  if (compare_names) {
    if (a.name() != b.name()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "names differ: %s / %s",
               a.name().c_str(), b.name().c_str());
      retval = false;
    }
  }
  if (a.linkcount() != b.linkcount()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "linkcounts differ: %lu / %lu",
             a.linkcount(), b.linkcount());
    retval = false;
  }
  if (a.hardlink_group() != b.hardlink_group()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "hardlink groups differ: %lu / %lu",
             a.hardlink_group(), b.hardlink_group());
    retval = false;
  }
  if (a.size() != b.size()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "sizes differ: %"PRIu64" / %"PRIu64,
             a.size(), b.size());
    retval = false;
  }
  if (a.mode() != b.mode()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "modes differ: %lu / %lu",
             a.mode(), b.mode());
    retval = false;
  }
  if (a.mtime() != b.mtime()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "timestamps differ: %lu / %lu",
             a.mtime(), b.mtime());
    retval = false;
  }
  if (a.checksum() != b.checksum()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "content hashes differ: %s / %s",
             a.checksum().ToString().c_str(), b.checksum().ToString().c_str());
    retval = false;
  }
  if (a.symlink() != b.symlink()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "symlinks differ: %s / %s",
             a.symlink().c_str(), b.symlink().c_str());
    retval = false;
  }

  return retval;
}


static bool CompareCounters(const catalog::Counters &a,
                            const catalog::Counters &b)
{
  bool retval = true;
  if (a.self_regular != b.self_regular) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of regular files differs (%"PRIu64" / %"PRIu64")",
             a.self_regular, b.self_regular);
    retval = false;
  }
  if (a.self_symlink != b.self_symlink) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of symlinks differs (%"PRIu64" / %"PRIu64")",
             a.self_symlink, b.self_symlink);
    retval = false;
  }
  if (a.self_dir != b.self_dir) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of directories differs (%"PRIu64" / %"PRIu64")",
             a.self_dir, b.self_dir);
    retval = false;
  }
  if (a.self_nested != b.self_nested) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of nested_catalogs differs (%"PRIu64" / %"PRIu64")",
             a.self_nested, b.self_nested);
    retval = false;
  }
  if (a.subtree_regular != b.subtree_regular) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of subtree regular files differs (%"PRIu64" / %"PRIu64")",
             a.subtree_regular, b.subtree_regular);
    retval = false;
  }
  if (a.subtree_symlink != b.subtree_symlink) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of subtree symlinks differs (%"PRIu64" / %"PRIu64")",
             a.subtree_symlink, b.subtree_symlink);
    retval = false;
  }
  if (a.subtree_symlink != b.subtree_symlink) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of subtree symlinks differs (%"PRIu64" / %"PRIu64")",
             a.subtree_symlink, b.subtree_symlink);
    retval = false;
  }
  if (a.subtree_dir != b.subtree_dir) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of subtree directories differs (%"PRIu64" / %"PRIu64")",
             a.subtree_dir, b.subtree_dir);
    retval = false;
  }
  if (a.subtree_nested != b.subtree_nested) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "number of subtree nested catalogs differs (%"PRIu64" / %"PRIu64")",
             a.subtree_nested, b.subtree_nested);
    retval = false;
  }

  return retval;
}


/**
 * Recursive catalog walk-through
 */
static bool Find(const catalog::Catalog *catalog,
                 PathString path, catalog::DeltaCounters *computed_counters)
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

    // Check if the chunk is there
    if (!entries[i].checksum().IsNull() && check_chunks) {
      string chunk_path = "data" + entries[i].checksum().MakePath(1, 2);
      if (entries[i].IsDirectory())
        chunk_path += "L";
      if (!FileExists(chunk_path)) {
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
      computed_counters->d_self_dir++;
      num_subdirs++;
      // Directory size
      if (entries[i].size() < 4096) {
        LogCvmfs(kLogCvmfs, kLogStderr, "invalid file size for %s",
                 full_path.c_str());
        retval = false;
      }
      // No directory hardlinks
      if (entries[i].hardlink_group() != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "directory hardlink found at %s",
                 full_path.c_str());
        retval = false;
      }
      if (entries[i].IsNestedCatalogMountpoint()) {
        // Find transition point
        computed_counters->d_self_nested++;
        hash::Any tmp;
        if (!catalog->FindNested(full_path, &tmp)) {
          LogCvmfs(kLogCvmfs, kLogStderr, "nested catalog at %s not registered",
                   full_path.c_str());
          retval = false;
        }
      } else {
        // Recurse
        if (!Find(catalog, full_path, computed_counters))
          retval = false;
      }
    } else if (entries[i].IsLink()) {
      computed_counters->d_self_symlink++;
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
      computed_counters->d_self_regular++;
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown file type %s",
               full_path.c_str());
      retval = false;
    }
  }

  // Check directory linkcount
  if (this_directory.linkcount() != num_subdirs + 2) {
    LogCvmfs(kLogCvmfs, kLogStderr, "wrong linkcount for %s; "
             "expected %lu, got %lu",
             path.c_str(), num_subdirs + 2, this_directory.linkcount());
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


static catalog::Catalog *DecompressCatalog(const string &path,
                                           const hash::Any catalog_hash)
{
  const string source = "data" + catalog_hash.MakePath(1,2) + "C";
  const string dest = "/tmp/" + catalog_hash.ToString();
  if (!zlib::DecompressPath2Path(source, dest))
    return NULL;

  catalog::Catalog *catalog =
    new catalog::Catalog(PathString(path.data(), path.length()), NULL);
  bool retval = catalog->OpenDatabase(dest);
  unlink(dest.c_str());
  if (!retval) {
    delete catalog;
    return NULL;
  }
  catalog::InodeRange inode_range;
  inode_range.offset = 256;
  inode_range.size = 256 + catalog->max_row_id();
  catalog->set_inode_range(inode_range);
  return catalog;
}


/**
 * Recursion on nested catalog level.  No ownership of computed_counters.
 */
static bool InspectTree(const string &path, const hash::Any &catalog_hash,
                        const catalog::DirectoryEntry *transition_point,
                        catalog::DeltaCounters *computed_counters)
{
  LogCvmfs(kLogCvmfs, kLogStdout, "[inspecting catalog] %s at %s",
           catalog_hash.ToString().c_str(), path == "" ? "/" : path.c_str());

  catalog::Catalog *catalog = DecompressCatalog(path, catalog_hash);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStdout, "failed to open catalog %s",
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
  if (transition_point != NULL) {
    if (!CompareEntries(*transition_point, root_entry, true)) {
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
    retval = false;

  // Check number of entries
  const uint64_t num_found_entries = 1 + computed_counters->d_self_regular +
    computed_counters->d_self_symlink + computed_counters->d_self_dir;
  if (num_found_entries != catalog->GetNumEntries()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "dangling entries in catalog, "
             "expected %"PRIu64", got %"PRIu64,
             catalog->GetNumEntries(), num_found_entries);
    retval = false;
  }

  // Recurse into nested catalogs
  catalog::Catalog::NestedCatalogList *nested_catalogs =
    catalog->ListNestedCatalogs();
  if (nested_catalogs->size() !=
      static_cast<uint64_t>(computed_counters->d_self_nested))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "number of nested catalogs does not match;"
             " expected %lu, got %lu", computed_counters->d_self_nested,
             nested_catalogs->size());
    retval = false;
  }
  for (catalog::Catalog::NestedCatalogList::const_iterator i =
       nested_catalogs->begin(), iEnd = nested_catalogs->end(); i != iEnd; ++i)
  {
    catalog::DirectoryEntry nested_transition_point;
    if (!catalog->LookupPath(i->path, &nested_transition_point)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup transition point %s",
               i->path.c_str());
      retval = false;
    } else {
      catalog::DeltaCounters nested_counters;
      if (!InspectTree(i->path.ToString(), i->hash, &nested_transition_point,
                       &nested_counters))
        retval = false;
      nested_counters.PopulateToParent(computed_counters);
    }
  }

  // Check statistics counters
  computed_counters->d_self_dir++;  // Additionally account for root directory
  catalog::Counters compare_counters;
  compare_counters.ApplyDelta(*computed_counters);
  catalog::Counters stored_counters;
  if (!catalog->GetCounters(&stored_counters)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to get counters (%s) [%s]",
             path.c_str(), catalog_hash.ToString().c_str());
    retval = false;
  } else {
    if (!CompareCounters(compare_counters, stored_counters)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "statistics counter mismatch [%s]",
               catalog_hash.ToString().c_str());
      retval = false;
    }
  }

  delete catalog;
  return retval;
}


int main(int argc, char **argv) {
  check_chunks = false;

  char c;
  while ((c = getopt(argc, argv, "l:ch")) != -1) {
    switch (c) {
      case 'l': {
        unsigned log_level = 1 << (kLogLevel0 + String2Uint64(optarg));
        if (log_level > kLogNone) {
          Usage();
          return false;
        }
        SetLogVerbosity(static_cast<LogLevels>(log_level));
        break;
      }
      case 'c':
        check_chunks = true;
        break;
      case 'h':
        Usage();
        return 0;
      case '?':
      default:
        Usage();
        return 1;
    }
  }

  if (optind >= argc) {
    Usage();
    return 1;
  }

  const string repo_dir = MakeCanonicalPath(argv[optind]);
  if (chdir(repo_dir.c_str()) != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to switch to directory %s",
             repo_dir.c_str());
    return 1;
  }

  Manifest *manifest = Manifest::LoadFile(".cvmfspublished");
  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return 1;
  }

  // Validate Manifest
  const string certificate_path =
    "data" + manifest->certificate().MakePath(1, 2) + "X";
  if (!FileExists(certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to find certificate (%s)",
             certificate_path.c_str());
    return 1;
  }

  catalog::DeltaCounters computed_counters;
  bool retval = InspectTree("", manifest->catalog_hash(), NULL,
                            &computed_counters);

  return retval ? 0 : 1;
}
