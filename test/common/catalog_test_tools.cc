/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include "catalog.h"
#include "compression.h"
#include "hash.h"
#include "manifest.h"
#include "receiver/params.h"
#include "testutil.h"
#include "util/posix.h"

namespace {

upload::Spooler* CreateSpooler(const std::string& config) {
  upload::SpoolerDefinition definition(config, shash::kSha1, zlib::kZlibDefault,
                                       false, true, 4194304, 8388608, 16777216,
                                       "dummy_token", "dummy_key");

  return upload::Spooler::Construct(definition);
}

catalog::WritableCatalogManager* CreateInputCatalogMgr(
    const std::string stratum0, const std::string& temp_dir,
    const bool volatile_content, upload::Spooler* spooler,
    download::DownloadManager* dl_mgr, perf::Statistics* stats) {
  UniquePtr<manifest::Manifest> manifest(
      catalog::WritableCatalogManager::CreateRepository(
          temp_dir, volatile_content, "", spooler));
  if (spooler->GetNumberOfErrors() > 0) {
    abort();
  }
  return new catalog::WritableCatalogManager(manifest->catalog_hash(), stratum0,
                                             temp_dir, spooler, dl_mgr, false,
                                             0, 0, 0, stats, false, 0, 0);
}
}

DirSpec::DirSpec(const DirSpecEntryList& entries) : entries_(entries) {}

DirSpec::~DirSpec() {}

CatalogTestTool::CatalogTestTool(const DirSpec& spec)
    : spec_(spec),
      old_spooler_(),
      new_spooler_(),
      temp_dir_(),
      old_catalog_mgr_(),
      new_catalog_mgr_() {
  InitDownloadManager(true);

  shash::Any old_root_hash(shash::kSha1);
  shash::Any new_root_hash(shash::kSha1);

  const std::string sandbox_root = GetCurrentWorkingDirectory();

  temp_dir_ = RaiiTempDir::Create(sandbox_root + "/catalog_test");

  const std::string old_stratum0 = temp_dir_->dir() + "/old_stratum0";
  MkdirDeep(old_stratum0 + "/data", 0777);
  MakeCacheDirectories(old_stratum0 + "/data", 0777);
  const std::string old_cat_mgr_temp = old_stratum0 + "/data/txn";

  const std::string new_stratum0 = temp_dir_->dir() + "/new_stratum0";
  MkdirDeep(new_stratum0 + "/data", 0777);
  MakeCacheDirectories(new_stratum0 + "/data", 0777);
  const std::string new_cat_mgr_temp = new_stratum0 + "/data/txn";

  old_spooler_ = CreateSpooler("local," + old_cat_mgr_temp + "," + old_stratum0);
  new_spooler_ = CreateSpooler("local," + new_cat_mgr_temp + "," + new_stratum0);

  old_catalog_mgr_ = CreateInputCatalogMgr(
      "file://" + old_stratum0, old_cat_mgr_temp, false, old_spooler_.weak_ref(),
      download_manager_.weak_ref(), &old_stats_);

  new_catalog_mgr_ = CreateInputCatalogMgr(
      "file://" + new_stratum0, new_cat_mgr_temp, false, new_spooler_.weak_ref(),
      download_manager_.weak_ref(), &new_stats_);


  old_catalog_mgr_->Init();
  //new_catalog_mgr_->Init();

  /*
  MockCatalog* root_catalog = catalog_mgr_->RetrieveRootCatalog();
  for (size_t i = 0; i < spec_.entries_.size(); ++i) {
    const DirSpecEntry& entry = spec_.entries_[i];
    root_catalog->AddFile(entry.hash_, entry.size_, entry.parent_,
                         entry.name_);
  }
  */
}

CatalogTestTool::~CatalogTestTool() {}
