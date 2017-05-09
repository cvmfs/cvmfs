/**
 * This file is part of the CernVM File System.
 */

#include "catalog_merge_tool.h"

#include "catalog.h"
#include "hash.h"
#include "logging.h"
#include "util/posix.h"

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

CatalogMergeTool::CatalogMergeTool(const std::string& repo_path,
                                   const std::string& old_root_hash,
                                   const std::string& new_root_hash,
                                   const std::string& base_root_hash,
                                   const std::string& temp_dir_prefix,
                                   download::DownloadManager* download_manager)
    : CatalogDiffTool(repo_path, old_root_hash, new_root_hash, temp_dir_prefix,
                      download_manager),
      base_root_hash_(base_root_hash) {}

CatalogMergeTool::~CatalogMergeTool() {}

bool CatalogMergeTool::Run(shash::Any* /*resulting_root_hash*/) {
  bool ret = CatalogDiffTool::Run();

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

  return ret;
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

}  // namespace receiver
