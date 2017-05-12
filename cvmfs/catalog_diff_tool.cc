/**
 * This file is part of the CernVM File System.
 */

#include "catalog_diff_tool.h"

#include "catalog.h"
#include "download.h"
#include "hash.h"
#include "logging.h"
#include "util/posix.h"

namespace {
const uint64_t kLastInode = uint64_t(-1);

void AppendFirstEntry(catalog::DirectoryEntryList* entry_list) {
  catalog::DirectoryEntry empty_entry;
  entry_list->push_back(empty_entry);
}

void AppendLastEntry(catalog::DirectoryEntryList* entry_list) {
  assert(!entry_list->empty());
  catalog::DirectoryEntry last_entry;
  last_entry.set_inode(kLastInode);
  entry_list->push_back(last_entry);
}

bool IsSmaller(const catalog::DirectoryEntry& a,
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

catalog::SimpleCatalogManager* OpenCatalogManager(
    const std::string& repo_path, const std::string& temp_dir,
    const std::string& root_hash, download::DownloadManager* download_manager,
    perf::Statistics* stats) {
  shash::Any hash(shash::MkFromSuffixedHexPtr(shash::HexPtr(root_hash)));
  catalog::SimpleCatalogManager* mgr = new catalog::SimpleCatalogManager(
      hash, repo_path, temp_dir, download_manager, stats, true);
  mgr->Init();

  return mgr;
}

}  // namespace

CatalogDiffTool::CatalogDiffTool(const std::string& repo_path,
                                 const std::string& old_root_hash,
                                 const std::string& new_root_hash,
                                 const std::string& temp_dir_prefix,
                                 download::DownloadManager* download_manager)
    : repo_path_(repo_path),
      old_root_hash_(old_root_hash),
      new_root_hash_(new_root_hash),
      temp_dir_prefix_(temp_dir_prefix),
      download_manager_(download_manager),
      old_catalog_mgr_(),
      new_catalog_mgr_() {}

CatalogDiffTool::~CatalogDiffTool() {
  RemoveTree(temp_dir_old_);
  RemoveTree(temp_dir_new_);
}

bool CatalogDiffTool::Init() {
  // Create a temp directory
  temp_dir_old_ = CreateTempDir(temp_dir_prefix_);
  temp_dir_new_ = CreateTempDir(temp_dir_prefix_);

  // Old catalog from release manager machine (before lease)
  old_catalog_mgr_ =
      OpenCatalogManager(repo_path_, temp_dir_old_, old_root_hash_,
                         download_manager_, &stats_old_);

  // New catalog from release manager machine (before lease)
  new_catalog_mgr_ =
      OpenCatalogManager(repo_path_, temp_dir_new_, new_root_hash_,
                         download_manager_, &stats_new_);

  if (!old_catalog_mgr_.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open old catalog");
    return false;
  }

  if (!new_catalog_mgr_.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open new catalog");
    return false;
  }

  return true;
}

bool CatalogDiffTool::Run(const PathString& path) {
  DiffRec(path);

  return true;
}

void CatalogDiffTool::DiffRec(const PathString& path) {
  catalog::DirectoryEntryList old_listing;
  AppendFirstEntry(&old_listing);
  old_catalog_mgr_->Listing(path, &old_listing);
  AppendLastEntry(&old_listing);

  catalog::DirectoryEntryList new_listing;
  AppendFirstEntry(&new_listing);
  new_catalog_mgr_->Listing(path, &new_listing);
  AppendLastEntry(&new_listing);

  unsigned i_from = 0, size_from = old_listing.size();
  unsigned i_to = 0, size_to = new_listing.size();
  while ((i_from < size_from) || (i_to < size_to)) {
    const catalog::DirectoryEntry old_entry = old_listing[i_from];
    const catalog::DirectoryEntry new_entry = new_listing[i_to];

    PathString old_path(path);
    old_path.Append("/", 1);
    old_path.Append(old_entry.name().GetChars(), old_entry.name().GetLength());
    PathString new_path(path);
    new_path.Append("/", 1);
    new_path.Append(new_entry.name().GetChars(), new_entry.name().GetLength());

    if (IsSmaller(new_entry, old_entry)) {
      i_to++;
      XattrList xattrs;
      if (new_entry.HasXattrs()) {
        new_catalog_mgr_->LookupXattrs(new_path, &xattrs);
      }
      ReportAddition(new_path, new_entry, xattrs);
      continue;
    } else if (IsSmaller(old_entry, new_entry)) {
      i_from++;
      ReportRemoval(old_path, old_entry);
      continue;
    }

    assert(old_path == new_path);
    i_from++;
    i_to++;
    if (old_entry.CompareTo(new_entry) > 0) {
      ReportModification(old_path, old_entry, new_entry);
    }
    if (!old_entry.IsDirectory() || !new_entry.IsDirectory()) continue;

    // Recursion
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

    DiffRec(old_path);
  }
}
