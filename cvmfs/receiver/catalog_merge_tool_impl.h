/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_CATALOG_MERGE_TOOL_IMPL_H_
#define CVMFS_RECEIVER_CATALOG_MERGE_TOOL_IMPL_H_

#include <string>

#include "catalog.h"
#include "hash.h"
#include "lease_path_util.h"
#include "logging.h"
#include "manifest.h"
#include "options.h"
#include "upload.h"
#include "util/posix.h"
#include "util/raii_temp_dir.h"

inline PathString MakeRelative(const PathString& path) {
  std::string rel_path;
  std::string abs_path = path.ToString();
  if (abs_path[0] == '/') {
    rel_path = abs_path.substr(1);
  } else {
    rel_path = abs_path;
  }
  return PathString(rel_path);
}

namespace receiver {

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::Run(
    const Params& params, std::string* new_manifest_path) {
  UniquePtr<upload::Spooler> spooler;
  perf::Statistics stats;
  UniquePtr<RaiiTempDir> raii_temp_dir(RaiiTempDir::Create(temp_dir_prefix_));
  if (needs_setup_) {
    upload::SpoolerDefinition definition(
        params.spooler_configuration, params.hash_alg, params.compression_alg,
        params.generate_legacy_bulk_chunks, params.use_file_chunking,
        params.min_chunk_size, params.avg_chunk_size, params.max_chunk_size,
        "dummy_token", "dummy_key");
    spooler = upload::Spooler::Construct(definition);
    const std::string temp_dir = raii_temp_dir->dir();
    output_catalog_mgr_ = new RwCatalogMgr(
        manifest_->catalog_hash(), repo_path_, temp_dir, spooler,
        download_manager_, params.enforce_limits, params.nested_kcatalog_limit,
        params.root_kcatalog_limit, params.file_mbyte_limit, &stats,
        params.use_autocatalogs, params.max_weight, params.min_weight);
    output_catalog_mgr_->Init();
  }

  bool ret = CatalogDiffTool<RoCatalogMgr>::Run(PathString(""));

  ret &= CreateNewManifest(new_manifest_path);

  output_catalog_mgr_.Destroy();

  return ret;
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
void CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportAddition(
    const PathString& path, const catalog::DirectoryEntry& entry,
    const XattrList& xattrs, const FileChunkList& chunks) {
  const PathString rel_path = MakeRelative(path);

  /*
   * Note: If the addition of a file or directory outside of the lease
   *       path is encountered here, this means that the item was deleted
   *       by another writer running concurrently.
   *       The correct course of action is to ignore this change here.
   * */
  if (!IsPathInLease(lease_path_, rel_path)) {
    return;
  }

  const std::string parent_path =
      std::strchr(rel_path.c_str(), '/') ? GetParentPath(rel_path).c_str() : "";

  if (entry.IsDirectory()) {
    output_catalog_mgr_->AddDirectory(entry, parent_path);
    if (entry.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    }
  } else if (entry.IsRegular() || entry.IsLink()) {
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&entry);
    if (entry.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }
  }
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
void CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportRemoval(
    const PathString& path, const catalog::DirectoryEntry& entry) {
  const PathString rel_path = MakeRelative(path);

  /*
   * Note: If the removal of a file or directory outside of the lease
   *       path is encountered here, this means that the item was created
   *       by another writer running concurrently.
   *       The correct course of action is to ignore this change here.
   * */
  if (!IsPathInLease(lease_path_, rel_path)) {
    return;
  }

  if (entry.IsDirectory()) {
    if (entry.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->RemoveNestedCatalog(std::string(rel_path.c_str()),
                                               false);
    }
    output_catalog_mgr_->RemoveDirectory(rel_path.c_str());
  } else if (entry.IsRegular() || entry.IsLink()) {
    output_catalog_mgr_->RemoveFile(rel_path.c_str());
  }
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
void CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportModification(
    const PathString& path, const catalog::DirectoryEntry& entry1,
    const catalog::DirectoryEntry& entry2, const XattrList& xattrs,
    const FileChunkList& chunks) {
  const PathString rel_path = MakeRelative(path);

  /*
   * Note: If the modification of a file or directory outside of the lease
   *       path is encountered here, this means that the item was modified
   *       by another writer running concurrently.
   *       The correct course of action is to ignore this change here.
   * */
  if (!IsPathInLease(lease_path_, rel_path)) {
    return;
  }

  const std::string parent_path =
      std::strchr(rel_path.c_str(), '/') ? GetParentPath(rel_path).c_str() : "";

  if (entry1.IsDirectory() && entry2.IsDirectory()) {
    // From directory to directory
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&entry2);
    output_catalog_mgr_->TouchDirectory(*base_entry, rel_path.c_str());
    if (!entry1.IsNestedCatalogMountpoint() &&
        entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    } else if (entry1.IsNestedCatalogMountpoint() &&
               !entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->RemoveNestedCatalog(std::string(rel_path.c_str()));
    }
  } else if ((entry1.IsRegular() || entry1.IsLink()) && entry2.IsDirectory()) {
    // From file to directory
    output_catalog_mgr_->RemoveFile(rel_path.c_str());
    output_catalog_mgr_->AddDirectory(entry2, parent_path);
    if (entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    }

  } else if (entry1.IsDirectory() && (entry2.IsRegular() || entry2.IsLink())) {
    // From directory to file
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&entry2);
    output_catalog_mgr_->RemoveDirectory(rel_path.c_str());
    if (entry2.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }

  } else if ((entry1.IsRegular() || entry1.IsLink()) &&
             (entry2.IsRegular() || entry2.IsLink())) {
    // From file to file
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&entry2);
    output_catalog_mgr_->RemoveFile(rel_path.c_str());
    if (entry2.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }
  }
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::CreateNewManifest(
    std::string* new_manifest_path) {
  if (!output_catalog_mgr_->Commit(false, 0, manifest_)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CatalogMergeTool - Could not commit output catalog");
    return false;
  }

  const std::string new_path = CreateTempPath(temp_dir_prefix_, 0600);

  if (!manifest_->Export(new_path)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CatalogMergeTool - Could not export new manifest");
  }

  *new_manifest_path = new_path;

  return true;
}

}  // namespace receiver

#endif  // CVMFS_RECEIVER_CATALOG_MERGE_TOOL_IMPL_H_
