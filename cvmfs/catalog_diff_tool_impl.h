/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_DIFF_TOOL_IMPL_H_
#define CVMFS_CATALOG_DIFF_TOOL_IMPL_H_

#include <algorithm>
#include <string>

#include "catalog.h"
#include "catalog_diff_tool.h"
#include "download.h"
#include "hash.h"
#include "logging.h"
#include "util/posix.h"

const uint64_t kLastInode = uint64_t(-1);

inline void AppendFirstEntry(catalog::DirectoryEntryList* entry_list) {
  catalog::DirectoryEntry empty_entry;
  entry_list->push_back(empty_entry);
}

inline void AppendLastEntry(catalog::DirectoryEntryList* entry_list) {
  assert(!entry_list->empty());
  catalog::DirectoryEntry last_entry;
  last_entry.set_inode(kLastInode);
  entry_list->push_back(last_entry);
}

inline bool IsSmaller(const catalog::DirectoryEntry& a,
                      const catalog::DirectoryEntry& b) {
  bool a_is_first = (a.inode() == catalog::DirectoryEntryBase::kInvalidInode);
  bool a_is_last = (a.inode() == kLastInode);
  bool b_is_first = (b.inode() == catalog::DirectoryEntryBase::kInvalidInode);
  bool b_is_last = (b.inode() == kLastInode);

  if (a_is_last || b_is_first) return false;
  if (a_is_first) return !b_is_first;
  if (b_is_last) return !a_is_last;
  return a.name() < b.name();
}

template <typename RoCatalogMgr>
bool CatalogDiffTool<RoCatalogMgr>::Init() {
  if (needs_setup_) {
    // Create a temp directory
    old_raii_temp_dir_ = RaiiTempDir::Create(temp_dir_prefix_);
    new_raii_temp_dir_ = RaiiTempDir::Create(temp_dir_prefix_);

    // Old catalog from release manager machine (before lease)
    old_catalog_mgr_ =
        OpenCatalogManager(repo_path_, old_raii_temp_dir_->dir(),
                           old_root_hash_, download_manager_, &stats_old_);

    // New catalog from release manager machine (before lease)
    new_catalog_mgr_ =
        OpenCatalogManager(repo_path_, new_raii_temp_dir_->dir(),
                           new_root_hash_, download_manager_, &stats_new_);

    if (!old_catalog_mgr_.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not open old catalog");
      return false;
    }

    if (!new_catalog_mgr_.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not open new catalog");
      return false;
    }
  }

  return true;
}

template <typename RoCatalogMgr>
bool CatalogDiffTool<RoCatalogMgr>::Run(const PathString& path) {
  DiffRec(path);

  return true;
}

template <typename RoCatalogMgr>
RoCatalogMgr* CatalogDiffTool<RoCatalogMgr>::OpenCatalogManager(
    const std::string& repo_path, const std::string& temp_dir,
    const shash::Any& root_hash, download::DownloadManager* download_manager,
    perf::Statistics* stats) {
  RoCatalogMgr* mgr = new RoCatalogMgr(root_hash, repo_path, temp_dir,
                                       download_manager, stats, true);
  mgr->Init();

  return mgr;
}

template <typename RoCatalogMgr>
void CatalogDiffTool<RoCatalogMgr>::DiffRec(const PathString& path) {
  LogCvmfs(kLogCvmfs, kLogSyslog, "CatalogDiffTool - DiffRec on %s",
           path.ToString().c_str());
  // This function is recursive at the very end, let's keep this in mind.

  // A directory entry is some layer of abstraction above a C struct dirent.
  // Nothing more than the type of file in the directory (can be another
  // directory), the name itself and the inode
  catalog::DirectoryEntryList old_listing;
  AppendFirstEntry(&old_listing);
  // We start allocating the DirectoryEntryList and we AppendFirstEntry to it.
  // It is nothing more than a vector where the first and the last entries are
  // marker.
  old_catalog_mgr_->Listing(path, &old_listing);
  // here we list the content of path into the old_listing DirectoryEntry.
  // I believe that this listing is not recursive, but I could be wrong here.
  // Non recursive listing may explain the recursiveness of this function.
  sort(old_listing.begin(), old_listing.end(), IsSmaller);
  // obvious, the IsSmaller just compares the names of the DirectoryEntriy-ies
  // being careful with the inode.
  AppendLastEntry(&old_listing);
  // We mark the old_listing as over.

  catalog::DirectoryEntryList new_listing;
  AppendFirstEntry(&new_listing);
  new_catalog_mgr_->Listing(path, &new_listing);
  sort(new_listing.begin(), new_listing.end(), IsSmaller);
  AppendLastEntry(&new_listing);
  // Here we repeat what we did above, excatly the same, but this time we use
  // the new_catalog_manager

  // At this point we have two DirectoryEntryList (vectors), that are sorted.
  // Both of them contains the files that are under the directory `path` (the
  // arguments) but at different points in times, before and after the
  // transaction.

  // Now we are starting the loop inside of which we call this same function
  // recursively
  unsigned i_from = 0, size_from = old_listing.size();
  unsigned i_to = 0, size_to = new_listing.size();
  while ((i_from < size_from) || (i_to < size_to)) {
    // the loops goes on as long as we don't have visited all the entries in
    // both lists
    catalog::DirectoryEntry old_entry = old_listing[i_from];
    catalog::DirectoryEntry new_entry = new_listing[i_to];

    // some sanity check
    if (old_entry.linkcount() == 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "CatalogDiffTool - Entry %s in old catalog has linkcount 0. "
               "Aborting.",
               old_entry.name().c_str());
      abort();
    }
    if (new_entry.linkcount() == 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "CatalogDiffTool - Entry %s in new catalog has linkcount 0. "
               "Aborting.",
               new_entry.name().c_str());
      abort();
    }

    // Skip .cvmfs hidden directory
    while (old_entry.IsHidden()) old_entry = old_listing[++i_from];
    while (new_entry.IsHidden()) new_entry = new_listing[++i_to];

    // From now on we work in `PathString`s not anymore simple strings.
    PathString old_path(path);
    old_path.Append("/", 1);
    old_path.Append(old_entry.name().GetChars(), old_entry.name().GetLength());
    PathString new_path(path);
    new_path.Append("/", 1);
    new_path.Append(new_entry.name().GetChars(), new_entry.name().GetLength());

    XattrList xattrs;
    if (new_entry.HasXattrs()) {
      new_catalog_mgr_->LookupXattrs(new_path, &xattrs);
    }

    // note the `continue` statement, we apply the differences and then we skip
    // to the next iteraction
    if (IsSmaller(new_entry, old_entry)) {
      // we found a difference, there is something new in the new listing.
      i_to++;
      FileChunkList chunks;
      if (new_entry.IsChunkedFile()) {
        new_catalog_mgr_->ListFileChunks(new_path, new_entry.hash_algorithm(),
                                         &chunks);
      }
      ReportAddition(new_path, new_entry, xattrs, chunks);
      if (new_entry.IsDirectory()) {
        // Recursion!
        DiffRec(new_path);
      }
      continue;
    } else if (IsSmaller(old_entry, new_entry)) {
      // another difference, there is something less in the new listing.
      i_from++;
      if (old_entry.IsDirectory() && !old_entry.IsNestedCatalogMountpoint()) {
        // Recursion! Again!
        DiffRec(old_path);
      }
      ReportRemoval(old_path, old_entry);
      continue;
    }

    // if we arrived here the dirent are the same, but the content could have
    // change
    assert(old_path == new_path);
    // let's not forget to upgrade our counters, later it will be more
    // difficult.
    i_from++;
    i_to++;

    // we are computing the differences between two entries
    // a diff of kIdentical (which is equal to zero), means that there are no
    // differences between the two entries.
    catalog::DirectoryEntryBase::Differences diff =
        old_entry.CompareTo(new_entry);
    if ((diff == catalog::DirectoryEntryBase::Difference::kIdentical) &&
        old_entry.IsNestedCatalogMountpoint()) {
      // Early recursion stop if nested catalogs are identical
      shash::Any id_nested_from, id_nested_to;
      id_nested_from = old_catalog_mgr_->GetNestedCatalogHash(old_path);
      id_nested_to = new_catalog_mgr_->GetNestedCatalogHash(new_path);
      assert(!id_nested_from.IsNull() && !id_nested_to.IsNull());
      if (id_nested_from == id_nested_to) continue;
    }

    // something is different
    if (old_entry.CompareTo(new_entry) > 0) {
      FileChunkList chunks;
      if (new_entry.IsChunkedFile()) {
        new_catalog_mgr_->ListFileChunks(new_path, new_entry.hash_algorithm(),
                                         &chunks);
      }
      ReportModification(old_path, old_entry, new_entry, xattrs, chunks);
    }
    // At least one of them is a directory
    // hence we need to recurse to the direcotries, one or both.
    if (!old_entry.IsDirectory() || !new_entry.IsDirectory()) {
      if (old_entry.IsDirectory()) {
        // Recursion!
        DiffRec(old_path);
      } else if (new_entry.IsDirectory()) {
        // Recursion!
        DiffRec(new_path);
      }
      continue;
    }

    // Recursion
    DiffRec(old_path);
  }
}

#endif  // CVMFS_CATALOG_DIFF_TOOL_IMPL_H_
