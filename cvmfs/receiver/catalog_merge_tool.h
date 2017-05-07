/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
#define CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_

#include <string>
#include <vector>

#include "catalog_diff_tool.h"
#include "util/pointer.h"

namespace download {
class DownloadManager;
}

namespace shash {
class Any;
}

namespace receiver {

class CatalogMergeTool : public CatalogDiffTool {
 public:
  CatalogMergeTool(const std::string& repo_name,
                   const std::string& old_root_hash,
                   const std::string& new_root_hash,
                   const std::string& base_root_hash,
                   const std::string& temp_dir_prefix,
                   download::DownloadManager* download_manager);
  virtual ~CatalogMergeTool();

  bool Run(shash::Any* resulting_root_hash);

 protected:
  virtual void ReportAddition(const PathString& path,
                              const catalog::DirectoryEntry& entry);
  virtual void ReportRemoval(const PathString& path,
                             const catalog::DirectoryEntry& entry);
  virtual void ReportModification(const PathString& path,
                                  const catalog::DirectoryEntry& old_entry,
                                  const catalog::DirectoryEntry& new_entry);

 private:
  struct ChangeItem {
    enum ChangeType { kAddition, kRemoval, kModification };
    ChangeItem(ChangeType type, const PathString& path,
               const catalog::DirectoryEntry& entry1);
    ChangeItem(ChangeType type, const PathString& path,
               const catalog::DirectoryEntry& entry1,
               const catalog::DirectoryEntry& entry2);
    ChangeItem(const ChangeItem& other);
    ~ChangeItem();

    ChangeType type_;
    PathString path_;
    const catalog::DirectoryEntry* entry1_;
    const catalog::DirectoryEntry* entry2_;
  };
  typedef std::vector<ChangeItem> ChangeList;

  std::string base_root_hash_;

  ChangeList changes_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
