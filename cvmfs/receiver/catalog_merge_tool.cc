/**
 * This file is part of the CernVM File System.
 */

#include "catalog_merge_tool.h"

#include "catalog.h"
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

catalog::SimpleCatalogManager* OpenCatalogManager(const std::string& repo_path,
                                                  const std::string& temp_dir,
                                                  const std::string& root_hash,
                                                  perf::Statistics* stats) {
  shash::Any hash(shash::MkFromSuffixedHexPtr(shash::HexPtr(root_hash)));
  catalog::SimpleCatalogManager* mgr = new catalog::SimpleCatalogManager(
      hash, repo_path, temp_dir, NULL, stats, true);
  mgr->Init();

  return mgr;
}

}  // namespace

namespace receiver {

CatalogMergeTool::ChangeItem::ChangeItem(ChangeType type,
                                         const PathString& path,
                                         const catalog::DirectoryEntry& entry1)
    : type_(type),
      path_(path),
      entry1_(new catalog::DirectoryEntry(entry1)),
      entry2_(NULL) {}

CatalogMergeTool::ChangeItem::ChangeItem(ChangeType type,
                                         const PathString& path,
                                         const catalog::DirectoryEntry& entry1,
                                         const catalog::DirectoryEntry& entry2)
    : type_(type),
      path_(path),
      entry1_(new catalog::DirectoryEntry(entry1)),
      entry2_(new catalog::DirectoryEntry(entry2)) {}

CatalogMergeTool::ChangeItem::ChangeItem(const ChangeItem& other)
    : type_(other.type_),
      path_(other.path_),
      entry1_(new catalog::DirectoryEntry(*(other.entry1_))),
      entry2_(other.entry2_ ? new catalog::DirectoryEntry(*(other.entry2_))
                            : NULL) {}

CatalogMergeTool::ChangeItem::~ChangeItem() {
  if (entry1_) {
    delete entry1_;
    entry1_ = NULL;
  }
  if (entry2_) {
    delete entry2_;
    entry2_ = NULL;
  }
}

CatalogMergeTool::CatalogMergeTool(const std::string& repo_name,
                                   const std::string& old_root_hash,
                                   const std::string& new_root_hash,
                                   const std::string& base_root_hash)
    : repo_path_("/srv/cvmfs/" + repo_name),
      old_root_hash_(old_root_hash),
      new_root_hash_(new_root_hash),
      base_root_hash_(base_root_hash),
      old_catalog_mgr_(),
      new_catalog_mgr_() {}

CatalogMergeTool::~CatalogMergeTool() {}

bool CatalogMergeTool::Merge(shash::Any* /*resulting_root_hash*/) {
  // Create a temp directory
  const std::string temp_dir = CreateTempDir("/tmp/cvmfs_receiver_merge_tool");

  // Old catalog from release manager machine (before lease)
  old_catalog_mgr_ =
      OpenCatalogManager(repo_path_, temp_dir, old_root_hash_, &stats_old_);

  // New catalog from release manager machine (before lease)
  new_catalog_mgr_ =
      OpenCatalogManager(repo_path_, temp_dir, new_root_hash_, &stats_new_);

  if (!old_catalog_mgr_.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open old catalog");
    return false;
  }

  if (!new_catalog_mgr_.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open new catalog");
    return false;
  }

  MergeRec(PathString(""));

  FILE* debug_file = std::fopen("/home/radu/debug.log", "w");
  for (size_t i = 0; i < changes_.size(); ++i) {
    const ChangeItem& change = changes_[i];
    fprintf(debug_file, "Change - type: %d, path: %s, entry1.name: %s",
            change.type_, change.path_.c_str(), change.entry1_->name().c_str());
    if (change.entry2_) {
      fprintf(debug_file, ", entry2.name: %s\n",
              change.entry2_->name().c_str());
    } else {
      fprintf(debug_file, "\n");
    }
  }
  std::fclose(debug_file);

  return true;
}

void CatalogMergeTool::ReportAddition(const PathString& path,
                                      const catalog::DirectoryEntry& entry) {
  changes_.push_back(ChangeItem(ChangeItem::kAddition, path, entry));
}

void CatalogMergeTool::ReportRemoval(const PathString& path,
                                     const catalog::DirectoryEntry& entry) {
  changes_.push_back(ChangeItem(ChangeItem::kRemoval, path, entry));
}

void CatalogMergeTool::ReportModification(
    const PathString& path, const catalog::DirectoryEntry& entry1,
    const catalog::DirectoryEntry& entry2) {
  changes_.push_back(
      ChangeItem(ChangeItem::kModification, path, entry1, entry2));
}

void CatalogMergeTool::MergeRec(const PathString& path) {
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
      ReportAddition(new_path, new_entry);
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

    MergeRec(old_path);
  }
}

}  // namespace receiver
