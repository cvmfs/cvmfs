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
#include "util/exception.h"
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

inline void SplitHardlink(catalog::DirectoryEntry* entry) {
  if (entry->linkcount() > 1) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
              "CatalogMergeTool - Hardlink found: %s. Hardlinks are not "
              "supported when publishing through repository gateway and "
              "will be split.", entry->name().c_str());
    entry->set_linkcount(1);
  }
}

inline void AbortIfHardlinked(const catalog::DirectoryEntry& entry) {
  if (entry.linkcount() > 1) {
    PANIC(kLogSyslogErr,
          "CatalogMergeTool - Removal of file %s with linkcount > 1 is "
          "not supported. Aborting",
          entry.name().c_str());
  }
}

namespace receiver {

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::Run(
    const Params& params, std::string* new_manifest_path, uint64_t *final_rev) {
  UniquePtr<upload::Spooler> spooler;
  perf::StatisticsTemplate stats_tmpl("publish", statistics_);
  counters_ = new perf::FsCounters(stats_tmpl);

  UniquePtr<RaiiTempDir> raii_temp_dir(RaiiTempDir::Create(temp_dir_prefix_));
  if (needs_setup_) {
    upload::SpoolerDefinition definition(
        params.spooler_configuration, params.hash_alg, params.compression_alg,
        params.generate_legacy_bulk_chunks, params.use_file_chunking,
        params.min_chunk_size, params.avg_chunk_size, params.max_chunk_size,
        "dummy_token", "dummy_key");
    spooler = upload::Spooler::Construct(definition, &stats_tmpl);
    const std::string temp_dir = raii_temp_dir->dir();
    output_catalog_mgr_ = new RwCatalogMgr(
        manifest_->catalog_hash(), repo_path_, temp_dir, spooler,
        download_manager_, params.enforce_limits, params.nested_kcatalog_limit,
        params.root_kcatalog_limit, params.file_mbyte_limit, statistics_,
        params.use_autocatalogs, params.max_weight, params.min_weight);
    output_catalog_mgr_->Init();
  }

  bool ret = CatalogDiffTool<RoCatalogMgr>::Run(PathString(""));

  ret &= CreateNewManifest(new_manifest_path);

  *final_rev = manifest_->revision();

  output_catalog_mgr_.Destroy();

  return ret;
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::IsIgnoredPath(
    const PathString& path) {
  const PathString rel_path = MakeRelative(path);

  // Ignore any paths that are not either within the lease path or
  // above the lease path
  return !(IsSubPath(lease_path_, rel_path) ||
           IsSubPath(rel_path, lease_path_));
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::IsReportablePath(
    const PathString& path) {
  const PathString rel_path = MakeRelative(path);

  // Do not report any changes occurring outside the lease path (which
  // will be due to other concurrent writers)
  return IsSubPath(lease_path_, rel_path);
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
void CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportAddition(
    const PathString& path, const catalog::DirectoryEntry& entry,
    const XattrList& xattrs, const FileChunkList& chunks) {
  const PathString rel_path = MakeRelative(path);

  const std::string parent_path =
      std::strchr(rel_path.c_str(), '/') ? GetParentPath(rel_path).c_str() : "";

  if (entry.IsDirectory()) {
    output_catalog_mgr_->AddDirectory(entry, xattrs, parent_path);
    if (entry.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    }
    perf::Inc(counters_->n_directories_added);
  } else if (entry.IsRegular() || entry.IsLink()) {
    catalog::DirectoryEntry modified_entry = entry;
    SplitHardlink(&modified_entry);
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&modified_entry);
    if (entry.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }
    if (entry.IsLink())
      perf::Inc(counters_->n_symlinks_added);
    else
      perf::Inc(counters_->n_files_added);
    perf::Xadd(counters_->sz_added_bytes, entry.size());
  }
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
void CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportRemoval(
    const PathString& path, const catalog::DirectoryEntry& entry) {
  const PathString rel_path = MakeRelative(path);

  if (entry.IsDirectory()) {
    if (entry.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->RemoveNestedCatalog(std::string(rel_path.c_str()),
                                               false);
    }

    output_catalog_mgr_->RemoveDirectory(rel_path.c_str());
    perf::Inc(counters_->n_directories_removed);
  } else if (entry.IsRegular() || entry.IsLink()) {
    AbortIfHardlinked(entry);
    output_catalog_mgr_->RemoveFile(rel_path.c_str());

    if (entry.IsLink())
      perf::Inc(counters_->n_symlinks_removed);
    else
      perf::Inc(counters_->n_files_removed);

    perf::Xadd(counters_->sz_removed_bytes, entry.size());
  }
}

template <typename RwCatalogMgr, typename RoCatalogMgr>
bool CatalogMergeTool<RwCatalogMgr, RoCatalogMgr>::ReportModification(
    const PathString& path, const catalog::DirectoryEntry& entry1,
    const catalog::DirectoryEntry& entry2, const XattrList& xattrs,
    const FileChunkList& chunks) {
  const PathString rel_path = MakeRelative(path);

  const std::string parent_path =
      std::strchr(rel_path.c_str(), '/') ? GetParentPath(rel_path).c_str() : "";

  if (entry1.IsNestedCatalogMountpoint() &&
      entry2.IsNestedCatalogMountpoint()) {
    // From nested catalog to nested catalog
    RoCatalogMgr *new_catalog_mgr =
      CatalogDiffTool<RoCatalogMgr>::GetNewCatalogMgr();
    PathString mountpoint;
    shash::Any new_hash;
    uint64_t new_size;
    const bool found = new_catalog_mgr->LookupNested(path, &mountpoint,
                                                     &new_hash, &new_size);
    if (!found || !new_size) {
      PANIC(kLogSyslogErr,
            "CatalogMergeTool - nested catalog %s not found. Aborting",
            rel_path.c_str());
    }
    output_catalog_mgr_->SwapNestedCatalog(rel_path.ToString(), new_hash,
                                           new_size);
    return false; // skip recursion into nested catalog mountpoints
  } else if (entry1.IsDirectory() && entry2.IsDirectory()) {
    // From directory to directory
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&entry2);
    output_catalog_mgr_->TouchDirectory(*base_entry, xattrs, rel_path.c_str());
    if (!entry1.IsNestedCatalogMountpoint() &&
        entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    } else if (entry1.IsNestedCatalogMountpoint() &&
               !entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->RemoveNestedCatalog(std::string(rel_path.c_str()));
    }
    perf::Inc(counters_->n_directories_changed);
  } else if ((entry1.IsRegular() || entry1.IsLink()) && entry2.IsDirectory()) {
    // From file to directory
    AbortIfHardlinked(entry1);
    output_catalog_mgr_->RemoveFile(rel_path.c_str());
    output_catalog_mgr_->AddDirectory(entry2, xattrs, parent_path);
    if (entry2.IsNestedCatalogMountpoint()) {
      output_catalog_mgr_->CreateNestedCatalog(std::string(rel_path.c_str()));
    }
    if (entry1.IsLink())
      perf::Inc(counters_->n_symlinks_removed);
    else
      perf::Inc(counters_->n_files_removed);
    perf::Xadd(counters_->sz_removed_bytes, entry1.size());
    perf::Inc(counters_->n_directories_added);

  } else if (entry1.IsDirectory() && (entry2.IsRegular() || entry2.IsLink())) {
    // From directory to file
    if (entry1.IsNestedCatalogMountpoint()) {
      // we merge the nested catalog with its parent, it will be the recursive
      // procedure that will take care of deleting all the files.
      output_catalog_mgr_->RemoveNestedCatalog(std::string(rel_path.c_str()),
                                               /* merge = */ true);
    }

    catalog::DirectoryEntry modified_entry = entry2;
    SplitHardlink(&modified_entry);
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&modified_entry);

    output_catalog_mgr_->RemoveDirectory(rel_path.c_str());

    if (entry2.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }

    perf::Inc(counters_->n_directories_removed);
    if (entry2.IsLink())
      perf::Inc(counters_->n_symlinks_added);
    else
      perf::Inc(counters_->n_files_added);
    perf::Xadd(counters_->sz_added_bytes, entry2.size());

  } else if ((entry1.IsRegular() || entry1.IsLink()) &&
             (entry2.IsRegular() || entry2.IsLink())) {
    // From file to file
    AbortIfHardlinked(entry1);
    catalog::DirectoryEntry modified_entry = entry2;
    SplitHardlink(&modified_entry);
    const catalog::DirectoryEntryBase* base_entry =
        static_cast<const catalog::DirectoryEntryBase*>(&modified_entry);
    output_catalog_mgr_->RemoveFile(rel_path.c_str());
    if (entry2.IsChunkedFile()) {
      assert(!chunks.IsEmpty());
      output_catalog_mgr_->AddChunkedFile(*base_entry, xattrs, parent_path,
                                          chunks);
    } else {
      output_catalog_mgr_->AddFile(*base_entry, xattrs, parent_path);
    }

    if (entry1.IsRegular() && entry2.IsRegular()) {
      perf::Inc(counters_->n_files_changed);
    } else if (entry1.IsRegular() && entry2.IsLink()) {
      perf::Inc(counters_->n_files_removed);
      perf::Inc(counters_->n_symlinks_added);
    } else if (entry1.IsLink() && entry2.IsRegular()) {
      perf::Inc(counters_->n_symlinks_removed);
      perf::Inc(counters_->n_files_added);
    } else {
      perf::Inc(counters_->n_symlinks_changed);
    }
    perf::Xadd(counters_->sz_removed_bytes, entry1.size());
    perf::Xadd(counters_->sz_added_bytes, entry2.size());
  }
  return true;
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
