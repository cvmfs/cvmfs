/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
#define CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_

#include <memory>
#include <string>

#include "catalog_diff_tool.h"
#include "file_chunk.h"
#include "params.h"
#include "shortstring.h"
#include "statistics.h"

namespace catalog {
class WritableCatalogManager;
}

namespace download {
class DownloadManager;
}

namespace manifest {
class Manifest;
}

namespace shash {
struct Any;
}

namespace receiver {

// Lease paths are slash-free and relative to the repository root. In
// particular, map "/" from mountless root ingests to the empty root path.
inline PathString NormalizeLeasePath(const PathString &lease_path) {
  const std::string path = lease_path.ToString();
  const size_t start = path.find_first_not_of('/');
  if (start == std::string::npos) {
    // Empty or all slashes (e.g. "" or "/"): the repository root.
    return PathString("");
  }
  const size_t end = path.find_last_not_of('/');
  return PathString(path.substr(start, end - start + 1));
}

template<typename RwCatalogMgr, typename RoCatalogMgr>
class CatalogMergeTool : public CatalogDiffTool<RoCatalogMgr> {
 public:
  CatalogMergeTool(RoCatalogMgr *old_catalog_mgr, RoCatalogMgr *new_catalog_mgr,
                   RwCatalogMgr *output_catalog_mgr,
                   const PathString &lease_path,
                   const std::string &temp_dir_prefix,
                   manifest::Manifest *manifest, perf::Statistics *statistics)
      : CatalogDiffTool<RoCatalogMgr>(old_catalog_mgr, new_catalog_mgr)
      , repo_path_("")
      , cache_dir_("")
      , lease_path_(NormalizeLeasePath(lease_path))
      , temp_dir_prefix_(temp_dir_prefix)
      , download_manager_(NULL)
      , manifest_(manifest)
      , output_catalog_mgr_(output_catalog_mgr)
      , needs_setup_(false)
      , statistics_(statistics)
      , counters_(nullptr) { }

  CatalogMergeTool(RoCatalogMgr *old_catalog_mgr, RoCatalogMgr *new_catalog_mgr,
                   const std::string &repo_path, const PathString &lease_path,
                   const std::string &temp_dir_prefix,
                   download::DownloadManager *download_manager,
                   manifest::Manifest *manifest, perf::Statistics *statistics)
      : CatalogDiffTool<RoCatalogMgr>(old_catalog_mgr, new_catalog_mgr)
      , repo_path_(repo_path)
      , cache_dir_("")
      , lease_path_(NormalizeLeasePath(lease_path))
      , temp_dir_prefix_(temp_dir_prefix)
      , download_manager_(download_manager)
      , manifest_(manifest)
      , needs_setup_(true)
      , statistics_(statistics)
      , counters_(nullptr) { }

  CatalogMergeTool(const std::string &repo_path,
                   const shash::Any &old_root_hash,
                   const shash::Any &new_root_hash,
                   const PathString &lease_path,
                   const std::string &temp_dir_prefix,
                   download::DownloadManager *download_manager,
                   manifest::Manifest *manifest,
                   perf::Statistics *statistics,
                   const std::string &cache_dir)
      : CatalogDiffTool<RoCatalogMgr>(repo_path, old_root_hash, new_root_hash,
                                      temp_dir_prefix, download_manager,
                                      cache_dir)
      , repo_path_(repo_path)
      , cache_dir_(cache_dir)
      , lease_path_(NormalizeLeasePath(lease_path))
      , temp_dir_prefix_(temp_dir_prefix)
      , download_manager_(download_manager)
      , manifest_(manifest)
      , needs_setup_(true)
      , statistics_(statistics)
      , counters_(nullptr) { }

  virtual ~CatalogMergeTool() { }

  bool Run(const Params &params, std::string *new_manifest_path,
           shash::Any *new_manifest_hash, uint64_t *final_rev);

 protected:
  virtual bool IsIgnoredPath(const PathString &path);
  virtual bool IsReportablePath(const PathString &path);

  virtual bool ReportAddition(const PathString &path,
                              const catalog::DirectoryEntry &entry,
                              const XattrList &xattrs,
                              const FileChunkList &chunks);
  virtual void ReportRemoval(const PathString &path,
                             const catalog::DirectoryEntry &entry);
  virtual bool ReportModification(const PathString &path,
                                  const catalog::DirectoryEntry &old_entry,
                                  const catalog::DirectoryEntry &new_entry,
                                  const XattrList &xattrs,
                                  const FileChunkList &chunks);

 private:
  bool CreateNewManifest(std::string *new_manifest_path);

  // Create missing, non-reportable lease ancestors before the scoped diff.
  // Their metadata is receiver-controlled; false indicates an invalid tree.

  bool CreateMissingAncestorDirs();

  std::string repo_path_;
  const std::string cache_dir_;  // path if local cache is used, otherwise empty

  PathString lease_path_;
  std::string temp_dir_prefix_;

  download::DownloadManager *download_manager_;

  manifest::Manifest *manifest_;

  std::unique_ptr<RwCatalogMgr> output_catalog_mgr_;

  const bool needs_setup_;

  perf::Statistics *statistics_;
  std::unique_ptr<perf::FsCounters> counters_;
};

}  // namespace receiver

#include "catalog_merge_tool_impl.h"

#endif  // CVMFS_RECEIVER_CATALOG_MERGE_TOOL_H_
