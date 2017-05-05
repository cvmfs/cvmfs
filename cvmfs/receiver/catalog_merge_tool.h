/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
#define CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_

#include <google/sparse_hash_map>

#include <string>

#include "catalog_mgr_ro.h"
#include "statistics.h"
#include "util/pointer.h"

namespace shash {
class Any;
}

namespace receiver {

class CatalogMergeTool {
 public:
  CatalogMergeTool(const std::string& repo_name,
                   const std::string& old_root_hash,
                   const std::string& new_root_hash,
                   const std::string& base_root_hash);
  ~CatalogMergeTool();

  bool Merge(shash::Any* resulting_root_hash);

  bool MergeRec(const PathString& path);

 private:
  typedef google::sparse_hash_map<PathString, catalog::DirectoryEntry*>
      DirEntryMap;

  std::string repo_name_;
  std::string old_root_hash_;
  std::string new_root_hash_;
  std::string base_root_hash_;

  perf::Statistics stats_;
  perf::Statistics stats_old_;
  perf::Statistics stats_new_;

  UniquePtr<catalog::SimpleCatalogManager> old_catalog_mgr_;
  UniquePtr<catalog::SimpleCatalogManager> new_catalog_mgr_;

  DirEntryMap old_entries_;

  FILE* debug_file_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
