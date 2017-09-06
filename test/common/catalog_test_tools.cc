/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include <gtest/gtest.h>

#include "catalog_rw.h"
#include "compression.h"
#include "hash.h"
#include "testutil.h"
#include "util/posix.h"

CatalogTestTool::CatalogTestTool(const std::string& name)
    : name_(name), stats_(), manifest_(), spooler_(), history_() {}

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

// Note: we always apply the dir spec to the revision corresponding to the original,
//       empty repository.
bool CatalogTestTool::Apply(const std::string& id, const DirSpec& spec) {
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
    CreateCatalogMgr(history_.front().second, "file://" + stratum0_,
                     temp_dir_, spooler_, download_manager(), &stats_));

  if (!catalog_mgr.IsValid()) {
    return false;
  }

  for (size_t i = 0; i < spec.size(); ++i) {
    const DirSpecItem& item = spec[i];
    if (item.entry_.IsRegular()) {
      catalog_mgr->AddFile(
          static_cast<const catalog::DirectoryEntryBase&>(item.entry_),
          item.xattrs_, item.parent_);
    } else if (item.entry_.IsDirectory()) {
      catalog_mgr->AddDirectory(
          static_cast<const catalog::DirectoryEntryBase&>(item.entry_),
          item.parent_);
    }
  }

  if (!catalog_mgr->Commit(false, 0, manifest_)) {
    return false;
  }

  history_.push_back(std::make_pair(id, manifest_->catalog_hash()));

  return true;
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
