/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_DIFF_TOOL_IMPL_H_
#define CVMFS_CATALOG_DIFF_TOOL_IMPL_H_

#include <algorithm>
#include <string>

#include "catalog.h"
#include "download.h"
#include "hash.h"
#include "logging.h"
#include "util/exception.h"
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

  // Terminate recursion upon reaching an ignored path
  if (IsIgnoredPath(path)) {
    assert(!IsReportablePath(path));
    return;
  }

  catalog::DirectoryEntryList old_listing;
  AppendFirstEntry(&old_listing);
  old_catalog_mgr_->Listing(path, &old_listing);
  sort(old_listing.begin(), old_listing.end(), IsSmaller);
  AppendLastEntry(&old_listing);

  catalog::DirectoryEntryList new_listing;
  AppendFirstEntry(&new_listing);
  new_catalog_mgr_->Listing(path, &new_listing);
  sort(new_listing.begin(), new_listing.end(), IsSmaller);
  AppendLastEntry(&new_listing);

  unsigned i_from = 0, size_from = old_listing.size();
  unsigned i_to = 0, size_to = new_listing.size();
  while ((i_from < size_from) || (i_to < size_to)) {
    catalog::DirectoryEntry old_entry = old_listing[i_from];
    catalog::DirectoryEntry new_entry = new_listing[i_to];

    if (old_entry.linkcount() == 0) {
      PANIC(kLogStderr,
            "CatalogDiffTool - Entry %s in old catalog has linkcount 0. "
            "Aborting.",
            old_entry.name().c_str());
    }
    if (new_entry.linkcount() == 0) {
      PANIC(kLogStderr,
            "CatalogDiffTool - Entry %s in new catalog has linkcount 0. "
            "Aborting.",
            new_entry.name().c_str());
    }

    // Skip .cvmfs hidden directory
    while (old_entry.IsHidden()) old_entry = old_listing[++i_from];
    while (new_entry.IsHidden()) new_entry = new_listing[++i_to];

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

    if (IsSmaller(new_entry, old_entry)) {
      i_to++;
      if (IsReportablePath(new_path)) {
        FileChunkList chunks;
        if (new_entry.IsChunkedFile()) {
          new_catalog_mgr_->ListFileChunks(new_path, new_entry.hash_algorithm(),
                                           &chunks);
        }
        ReportAddition(new_path, new_entry, xattrs, chunks);
      }
      if (new_entry.IsDirectory()) {
        DiffRec(new_path);
      }
      continue;
    } else if (IsSmaller(old_entry, new_entry)) {
      i_from++;
      if (old_entry.IsDirectory() && !old_entry.IsNestedCatalogMountpoint()) {
        DiffRec(old_path);
      }
      if (IsReportablePath(old_path)) {
        ReportRemoval(old_path, old_entry);
      }
      continue;
    }

    assert(old_path == new_path);
    i_from++;
    i_to++;

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

    if (IsReportablePath(old_path) &&
        ((diff != catalog::DirectoryEntryBase::Difference::kIdentical) ||
         old_entry.IsNestedCatalogMountpoint())) {
      // Modified directory entry, or nested catalog with modified hash
      FileChunkList chunks;
      if (new_entry.IsChunkedFile()) {
        new_catalog_mgr_->ListFileChunks(new_path, new_entry.hash_algorithm(),
                                         &chunks);
      }
      bool recurse =
        ReportModification(old_path, old_entry, new_entry, xattrs, chunks);
      if (!recurse) continue;
    }

    if (!old_entry.IsDirectory() || !new_entry.IsDirectory()) {
      if (old_entry.IsDirectory()) {
        DiffRec(old_path);
      } else if (new_entry.IsDirectory()) {
        DiffRec(new_path);
      }
      continue;
    }

    // Recursion
    DiffRec(old_path);
  }
}

#endif  // CVMFS_CATALOG_DIFF_TOOL_IMPL_H_
