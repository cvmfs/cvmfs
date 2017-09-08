/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include <gtest/gtest.h>

#include <sstream>

#include "catalog_rw.h"
#include "compression.h"
#include "directory_entry.h"
#include "hash.h"
#include "testutil.h"
#include "util/posix.h"

namespace {

bool ExportDirSpec(const std::string& path,
                   catalog::WritableCatalogManager* mgr, DirSpec* spec) {
  catalog::DirectoryEntryList listing;
  if (!mgr->Listing(path, &listing)) {
    return false;
  }

  for (size_t i = 0u; i < listing.size(); ++i) {
    const catalog::DirectoryEntry& entry = listing[i];
    const std::string entry_full_path = entry.GetFullPath(path);
    XattrList xattrs;
    if (entry.HasXattrs()) {
      mgr->LookupXattrs(PathString(entry_full_path), &xattrs);
    }
    spec->AddDirectoryEntry(entry, xattrs, path);
    if (entry.IsDirectory()) {
      if (!ExportDirSpec(entry_full_path, mgr, spec)) {
        return false;
      }
    }
  }

  return true;
}

}  // namespace

void DirSpec::AddFile(const std::string& name, const std::string& parent,
                      const std::string& digest, const size_t size,
                      const XattrList& xattrs, shash::Suffix suffix) {
  shash::Any hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(digest.c_str()), suffix);
  items_.push_back(DirSpecItem(catalog::DirectoryEntryTestFactory::RegularFile(name, size, hash),
                               xattrs, parent));
}

void DirSpec::AddDirectory(const std::string& name, const std::string& parent,
                           const size_t size) {
  items_.push_back(DirSpecItem(catalog::DirectoryEntryTestFactory::Directory(name, size),
                               XattrList(), parent));
}

void DirSpec::AddDirectoryEntry(const catalog::DirectoryEntry& entry,
                         const XattrList& xattrs,
                                const std::string& parent) {
  items_.push_back(DirSpecItem(entry, xattrs, parent));
}

void DirSpec::ToString(std::string* out) {
  std::ostringstream ostr;
  for (size_t i = 0u; i < NumItems(); ++i) {
    const DirSpecItem& item = Item(i);
    char item_type = ' ';
    if (item.entry_base().IsRegular()) {
      item_type = 'F';
    } else if (item.entry_base().IsDirectory()) {
      item_type = 'D';
    }
    std::string parent = item.parent();
    if (parent != "" && parent[0] != '/') {
      parent.insert(0, 1, '/');
    }

    ostr << item_type << " " << item.entry_base().GetFullPath(parent).c_str()
         << std::endl;
  }
  *out = ostr.str();
}

static bool CompareFunction(const DirSpecItem& item1, const DirSpecItem& item2) {
  std::string path1 = item1.entry_base().GetFullPath(item1.parent());
  std::string path2 = item2.entry_base().GetFullPath(item2.parent());
  if (path1[0] != '/') {
    path1.insert(0, 1, '/');
  }
  if (path2[0] != '/') {
    path2.insert(0, 1, '/');
  }
  return strcmp(path1.c_str(), path2.c_str()) < 0;
}

void DirSpec::Sort() {
  std::sort(items_.begin(), items_.end(), CompareFunction);
}

CatalogTestTool::CatalogTestTool(const std::string& name)
    : name_(name), manifest_(), spooler_(), history_() {}

bool CatalogTestTool::Init() {
  if (!InitDownloadManager(true)) {
    return false;
  }

  const std::string sandbox_root = GetCurrentWorkingDirectory();

  stratum0_ = sandbox_root + "/" + name_ + "_stratum0";
  MkdirDeep(stratum0_ + "/data", 0777);
  MakeCacheDirectories(stratum0_ + "/data", 0777);
  temp_dir_ = stratum0_ + "/data/txn";

  spooler_ = CreateSpooler("local," + temp_dir_ + "," + stratum0_);
  if (!spooler_.IsValid()) {
    return false;
  }

  manifest_ = CreateRepository(temp_dir_, spooler_);

  if (!manifest_.IsValid()) {
    return false;
  }

  history_.clear();
  history_.push_back(std::make_pair("initial", manifest_->catalog_hash()));

  return true;
}

// Note: we always apply the dir spec to the revision corresponding to the
// original,
//       empty repository.
bool CatalogTestTool::Apply(const std::string& id, const DirSpec& spec) {
  perf::Statistics stats;
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
      CreateCatalogMgr(history_.front().second, "file://" + stratum0_,
                       temp_dir_, spooler_, download_manager(), &stats));

  if (!catalog_mgr.IsValid()) {
    return false;
  }

  for (size_t i = 0; i < spec.NumItems(); ++i) {
    const DirSpecItem& item = spec.Item(i);
    if (item.entry_.IsRegular()) {
      catalog_mgr->AddFile(item.entry_base(), item.xattrs(), item.parent());
    } else if (item.entry_.IsDirectory()) {
      catalog_mgr->AddDirectory(item.entry_base(), item.parent());
    }
  }

  if (!catalog_mgr->Commit(false, 0, manifest_)) {
    return false;
  }

  history_.push_back(std::make_pair(id, manifest_->catalog_hash()));

  return true;
}

bool CatalogTestTool::DirSpecAtRootHash(const shash::Any& root_hash,
                                        DirSpec* spec) {
  perf::Statistics stats;
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
      CreateCatalogMgr(root_hash, "file://" + stratum0_, temp_dir_, spooler_,
                       download_manager(), &stats));

  if (!catalog_mgr.IsValid()) {
    return false;
  }

  return ExportDirSpec("", catalog_mgr.weak_ref(), spec);
}

CatalogTestTool::~CatalogTestTool() {}

upload::Spooler* CatalogTestTool::CreateSpooler(const std::string& config) {
  upload::SpoolerDefinition definition(config, shash::kSha1, zlib::kZlibDefault,
                                       false, true, 4194304, 8388608, 16777216,
                                       "dummy_token", "dummy_key");
  return upload::Spooler::Construct(definition);
}

manifest::Manifest* CatalogTestTool::CreateRepository(
    const std::string& dir, upload::Spooler* spooler) {
  manifest::Manifest* manifest =
      catalog::WritableCatalogManager::CreateRepository(dir, false, "",
                                                        spooler);
  if (spooler->GetNumberOfErrors() > 0) {
    return NULL;
  }

  return manifest;
}

catalog::WritableCatalogManager* CatalogTestTool::CreateCatalogMgr(
    const shash::Any& root_hash, const std::string stratum0,
    const std::string& temp_dir, upload::Spooler* spooler,
    download::DownloadManager* dl_mgr, perf::Statistics* stats) {
  catalog::WritableCatalogManager* catalog_mgr =
      new catalog::WritableCatalogManager(root_hash, stratum0, temp_dir,
                                          spooler, dl_mgr, false, 0, 0, 0,
                                          stats, false, 0, 0);
  catalog_mgr->Init();

  return catalog_mgr;
}
