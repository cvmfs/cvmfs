/**
 * This file is part of the CernVM File System.
 *
 * This tool checks a cvmfs cache directory for consistency.
 * If necessary, the managed cache db is removed so that
 * it will be rebuilt on next mount.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"

#include <unistd.h>
#include <inttypes.h>

#include <string>
#include <queue>

#include "logging.h"
#include "manifest.h"
#include "util.h"
#include "hash.h"
#include "catalog.h"
#include "compression.h"
#include "shortstring.h"

using namespace std;  // NOLINT

struct Counters {
  Counters() {
    num_files = 0;
    num_symlinks = 0;
    num_dirs = 0;
    num_transition_points = 0;
  }

  uint64_t num_files;
  uint64_t num_symlinks;
  uint64_t num_dirs;
  uint64_t num_transition_points;
};


static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM File System repository sanity checker, version %s\n\n"
    "This tool checks the consisteny of the file catalogs of a cvmfs repository."
    "\n\n"
    "Usage: cvmfs_check [options] <repository directory>\n"
    "Options:\n"
    "  -l  log level (0-4, default: 2)>\n",
    VERSION);
}


static bool CompareEntries(const catalog::DirectoryEntry &a,
                           const catalog::DirectoryEntry &b)
{
  bool retval = true;
  if (a.name() != b.name()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "names differ: %s / %s",
             a.name().c_str(), b.name().c_str());
    retval = false;
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


static bool Find(const catalog::Catalog *catalog,
                 PathString path, Counters *counters)
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

  for (unsigned i = 0; i < entries.size(); ++i) {
    PathString full_path(path);
    full_path.Append("/", 1);
    full_path.Append(entries[i].name().GetChars(),
                     entries[i].name().GetLength());
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "[path] %s",
             full_path.c_str());

    // Check if the chunk is there
    if (!entries[i].checksum().IsNull()) {
      string chunk_path = "data" + entries[i].checksum().MakePath(1, 2);
      if (entries[i].IsDirectory())
        chunk_path += "L";
      if (!FileExists(chunk_path)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "data chunk %s (%s) missing",
                 entries[i].checksum().ToString().c_str(), full_path.c_str());
        retval = false;
      }
    }

    // Checks depending of entry type
    if (entries[i].IsDirectory()) {
      counters->num_dirs++;
      num_subdirs++;
      // Directory size
      if (entries[i].size() != 4096) {
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
      // Recurse
      if (entries[i].IsNestedCatalogMountpoint()) {
        counters->num_transition_points++;
        hash::Any tmp;
        if (!catalog->FindNested(full_path, &tmp)) {
          LogCvmfs(kLogCvmfs, kLogStderr, "nested catalog at %s not registered",
                   full_path.c_str());
          retval = false;
        }
      } else {
        if (!Find(catalog, full_path, counters))
          retval = false;
      }
    } else if (entries[i].IsLink()) {
      counters->num_symlinks++;
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
      counters->num_files++;
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


static bool InspectTree(const string &path, const hash::Any &catalog_hash,
                        const catalog::DirectoryEntry *transition_point)
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
    if (!CompareEntries(*transition_point, root_entry)) {
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
  Counters *counters = new Counters();
  if (!Find(catalog, PathString(path.data(), path.length()), counters))
    retval = false;

  // Recurse into nested catalogs
  catalog::Catalog::NestedCatalogList nested_catalogs =
    catalog->ListNestedCatalogs();
  if (nested_catalogs.size() != counters->num_transition_points) {
    LogCvmfs(kLogCvmfs, kLogStderr, "number of nested catalogs does not match;"
             " expected %s, got %s", counters->num_transition_points,
             nested_catalogs.size());
    retval = false;
  }
  for (catalog::Catalog::NestedCatalogList::const_iterator i = nested_catalogs.begin(),
       iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    catalog::DirectoryEntry nested_transition_point;
    if (!catalog->LookupPath(i->path, &nested_transition_point)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to lookup transition point %s",
               i->path.c_str());
      retval = false;
    } else {
      if (!InspectTree(i->path.ToString(), i->hash, &nested_transition_point))
        retval = false;
    }
  }

  delete counters;
  delete catalog;
  return retval;
}


int main(int argc, char **argv) {
  char c;
  while ((c = getopt(argc, argv, "l:h")) != -1) {
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

  bool retval = InspectTree("", manifest->catalog_hash(), NULL);

  return retval ? 0 : 1;
}
