/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_DIFF_TOOL_H_
#define CVMFS_CATALOG_DIFF_TOOL_H_

#include <string>

#include "directory_entry.h"
#include "file_chunk.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/pointer.h"
#include "util/raii_temp_dir.h"
#include "xattr.h"

namespace download {
class DownloadManager;
}

template <typename RoCatalogMgr>
class CatalogDiffTool {
 public:
  CatalogDiffTool(RoCatalogMgr* old_catalog_mgr, RoCatalogMgr* new_catalog_mgr)
      : repo_path_(""),
        temp_dir_prefix_(""),
        download_manager_(NULL),
        old_catalog_mgr_(old_catalog_mgr),
        new_catalog_mgr_(new_catalog_mgr),
        needs_setup_(false) {}

  CatalogDiffTool(const std::string& repo_path, const shash::Any& old_root_hash,
                  const shash::Any& new_root_hash,
                  const std::string& temp_dir_prefix,
                  download::DownloadManager* download_manager)
      : repo_path_(repo_path),
        old_root_hash_(old_root_hash),
        new_root_hash_(new_root_hash),
        temp_dir_prefix_(temp_dir_prefix),
        download_manager_(download_manager),
        old_raii_temp_dir_(),
        new_raii_temp_dir_(),
        old_catalog_mgr_(),
        new_catalog_mgr_(),
        needs_setup_(true) {}

  virtual ~CatalogDiffTool() {}

  bool Init();

  bool Run(const PathString& path);

 protected:
  virtual void ReportAddition(const PathString& path,
                              const catalog::DirectoryEntry& entry,
                              const XattrList& xattrs,
                              const FileChunkList& chunks) = 0;
  virtual void ReportRemoval(const PathString& path,
                             const catalog::DirectoryEntry& entry) = 0;
  virtual void ReportModification(const PathString& path,
                                  const catalog::DirectoryEntry& old_entry,
                                  const catalog::DirectoryEntry& new_entry,
                                  const XattrList& xattrs,
                                  const FileChunkList& chunks) = 0;

  const catalog::Catalog* GetOldCatalog() const {
    return old_catalog_mgr_->GetRootCatalog();
  }
  const catalog::Catalog* GetNewCatalog() const {
    return new_catalog_mgr_->GetRootCatalog();
  }

 private:
  RoCatalogMgr* OpenCatalogManager(const std::string& repo_path,
                                   const std::string& temp_dir,
                                   const shash::Any& root_hash,
                                   download::DownloadManager* download_manager,
                                   perf::Statistics* stats);

  void DiffRec(const PathString& path);

  std::string repo_path_;
  shash::Any old_root_hash_;
  shash::Any new_root_hash_;
  std::string temp_dir_prefix_;

  download::DownloadManager* download_manager_;

  perf::Statistics stats_old_;
  perf::Statistics stats_new_;

  UniquePtr<RaiiTempDir> old_raii_temp_dir_;
  UniquePtr<RaiiTempDir> new_raii_temp_dir_;

  UniquePtr<RoCatalogMgr> old_catalog_mgr_;
  UniquePtr<RoCatalogMgr> new_catalog_mgr_;

  const bool needs_setup_;
};

#include "catalog_diff_tool_impl.h"

#endif  // CVMFS_CATALOG_DIFF_TOOL_H_
