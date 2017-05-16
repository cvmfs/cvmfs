/**
 * This file is part of the CernVM File System.
 */

#include "catalog_merge_tool.h"

#include "catalog.h"
#include "hash.h"
#include "logging.h"
#include "manifest.h"
#include "options.h"
#include "upload.h"
#include "util/posix.h"

namespace {

PathString MakeRelative(const PathString& path) {
  std::string rel_path;
  std::string abs_path = path.ToString();
  if (abs_path[0] == '/') {
    rel_path = abs_path.substr(1);
  } else {
    rel_path = abs_path;
  }
  return PathString(rel_path);
}

}  // namespace

namespace receiver {

CatalogMergeTool::ChangeItem::ChangeItem(ChangeType type,
                                         const PathString& path,
                                         const catalog::DirectoryEntry& entry1)
    : type_(type),
      path_(path),
      xattrs_(),
      entry1_(new catalog::DirectoryEntry(entry1)),
      entry2_(NULL) {}

CatalogMergeTool::ChangeItem::ChangeItem(ChangeType type,
                                         const PathString& path,
                                         const catalog::DirectoryEntry& entry1,
                                         const XattrList& xattrs)
    : type_(type),
      path_(path),
      xattrs_(xattrs),
      entry1_(new catalog::DirectoryEntry(entry1)),
      entry2_(NULL) {}

CatalogMergeTool::ChangeItem::ChangeItem(ChangeType type,
                                         const PathString& path,
                                         const catalog::DirectoryEntry& entry1,
                                         const catalog::DirectoryEntry& entry2)
    : type_(type),
      path_(path),
      xattrs_(),
      entry1_(new catalog::DirectoryEntry(entry1)),
      entry2_(new catalog::DirectoryEntry(entry2)) {}

CatalogMergeTool::ChangeItem::ChangeItem(const ChangeItem& other)
    : type_(other.type_),
      path_(other.path_),
      xattrs_(other.xattrs_),
      entry1_(new catalog::DirectoryEntry(*(other.entry1_))),
      entry2_(other.entry2_ ? new catalog::DirectoryEntry(*(other.entry2_))
                            : NULL) {}

CatalogMergeTool::ChangeItem::~ChangeItem() {
  delete entry1_;
  delete entry2_;
}

CatalogMergeTool::ChangeItem& CatalogMergeTool::ChangeItem::operator=(
    const ChangeItem& other) {
  type_ = other.type_;
  path_ = other.path_;
  xattrs_ = other.xattrs_;
  entry1_ = new catalog::DirectoryEntry(*other.entry1_);
  entry2_ = new catalog::DirectoryEntry(*other.entry2_);

  return *this;
}

CatalogMergeTool::CatalogMergeTool(const std::string& repo_path,
                                   const shash::Any& old_root_hash,
                                   const shash::Any& new_root_hash,
                                   const std::string& temp_dir_prefix,
                                   download::DownloadManager* download_manager,
                                   manifest::Manifest* manifest)
    : CatalogDiffTool(repo_path, old_root_hash, new_root_hash, temp_dir_prefix,
                      download_manager),
      repo_path_(repo_path),
      temp_dir_prefix_(temp_dir_prefix),
      download_manager_(download_manager),
      manifest_(manifest) {}

CatalogMergeTool::~CatalogMergeTool() {}

bool CatalogMergeTool::Run(const Params& params,
                           std::string* new_manifest_path) {
  bool ret = CatalogDiffTool::Run(PathString(""));

  upload::SpoolerDefinition definition(
      params.spooler_configuration, params.hash_alg, params.compression_alg,
      params.use_file_chunking, params.min_chunk_size, params.avg_chunk_size,
      params.max_chunk_size, "dummy_token", "dummy_key");
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(definition));
  perf::Statistics stats;
  const std::string temp_dir = CreateTempDir(temp_dir_prefix_);
  output_catalog_mgr_ = new catalog::WritableCatalogManager(
      manifest_->catalog_hash(), repo_path_, temp_dir, spooler,
      download_manager_, params.entry_warn_thresh, &stats,
      params.use_autocatalogs, params.max_weight, params.min_weight);
  output_catalog_mgr_->Init();

  ret &= InsertChangesIntoOutputCatalog();

  ret &= CreateNewManifest(new_manifest_path);

  return ret;
}

void CatalogMergeTool::ReportAddition(const PathString& path,
                                      const catalog::DirectoryEntry& entry,
                                      const XattrList& xattrs) {
  changes_.push_back(
      ChangeItem(ChangeItem::kAddition, MakeRelative(path), entry, xattrs));
}

void CatalogMergeTool::ReportRemoval(const PathString& path,
                                     const catalog::DirectoryEntry& entry) {
  changes_.push_back(
      ChangeItem(ChangeItem::kRemoval, MakeRelative(path), entry));
}

void CatalogMergeTool::ReportModification(
    const PathString& path, const catalog::DirectoryEntry& entry1,
    const catalog::DirectoryEntry& entry2) {
  changes_.push_back(ChangeItem(ChangeItem::kModification, MakeRelative(path),
                                entry1, entry2));
}

bool CatalogMergeTool::InsertChangesIntoOutputCatalog() {
  for (size_t i = 0; i < changes_.size(); ++i) {
    ChangeItem change = changes_[i];
    const std::string parent_path = std::strchr(change.path_.c_str(), '/')
                                        ? GetParentPath(change.path_).c_str()
                                        : "";
    switch (change.type_) {
      case ChangeItem::kAddition:

        if (change.entry1_->IsDirectory()) {
          output_catalog_mgr_->AddDirectory(*change.entry1_, parent_path);
        } else if (change.entry1_->IsRegular() || change.entry1_->IsLink()) {
          const catalog::DirectoryEntryBase* base_entry =
              static_cast<const catalog::DirectoryEntryBase*>(change.entry1_);
          output_catalog_mgr_->AddFile(*base_entry, change.xattrs_,
                                       parent_path);
        }
        break;

      case ChangeItem::kRemoval:

        if (change.entry1_->IsDirectory()) {
          output_catalog_mgr_->RemoveDirectory(change.path_.c_str());
        } else if (change.entry1_->IsRegular() || change.entry1_->IsLink()) {
          output_catalog_mgr_->RemoveFile(change.path_.c_str());
        }
        break;

      case ChangeItem::kModification:

        if (change.entry1_->IsDirectory() && change.entry2_->IsDirectory()) {
          // From directory to directory
          const catalog::DirectoryEntryBase* base_entry =
              static_cast<const catalog::DirectoryEntryBase*>(change.entry2_);
          output_catalog_mgr_->TouchDirectory(*base_entry,
                                              change.path_.c_str());

        } else if ((change.entry1_->IsRegular() || change.entry1_->IsLink()) &&
                   change.entry2_->IsDirectory()) {
          // From file to directory
          output_catalog_mgr_->RemoveFile(change.path_.c_str());
          output_catalog_mgr_->AddDirectory(*change.entry2_, parent_path);

        } else if (change.entry1_->IsDirectory() &&
                   (change.entry2_->IsRegular() || change.entry2_->IsLink())) {
          // From directory to file
          const catalog::DirectoryEntryBase* base_entry =
              static_cast<const catalog::DirectoryEntryBase*>(change.entry2_);
          output_catalog_mgr_->RemoveDirectory(change.path_.c_str());
          output_catalog_mgr_->AddFile(*base_entry, change.xattrs_,
                                       parent_path);

        } else if ((change.entry1_->IsRegular() || change.entry1_->IsLink()) &&
                   (change.entry2_->IsRegular() || change.entry2_->IsLink())) {
          // From file to file
          const catalog::DirectoryEntryBase* base_entry =
              static_cast<const catalog::DirectoryEntryBase*>(change.entry2_);
          output_catalog_mgr_->RemoveFile(change.path_.c_str());
          output_catalog_mgr_->AddFile(*base_entry, change.xattrs_,
                                       parent_path);
        }
        break;

      default:

        LogCvmfs(kLogCvmfs, kLogStderr,
                 "What should not be representable presented itself. Exiting.");
        abort();
        break;
    }
  }

  return true;
}

bool CatalogMergeTool::CreateNewManifest(std::string* new_manifest_path) {
  if (!output_catalog_mgr_->Commit(false, 0, manifest_)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "CatalogMergeTool - Could not commit output catalog");
    return false;
  }

  const std::string temp_dir = CreateTempDir(temp_dir_prefix_);
  const std::string new_path = temp_dir + "/new_manifest";

  if (!manifest_->Export(new_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "CatalogMergeTool - Could not export new manifest");
  }

  *new_manifest_path = new_path;

  return true;
}

}  // namespace receiver
