/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include <gtest/gtest.h>

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
  EXPECT_EQ(0, spooler->GetNumberOfErrors());

  return new catalog::WritableCatalogManager(manifest->catalog_hash(), stratum0,
                                             temp_dir, spooler, dl_mgr, false,
                                             0, 0, 0, stats, false, 0, 0);
}
}

DirSpec::DirSpec(const DirSpecEntryList& entries) : entries_(entries) {}

DirSpec::~DirSpec() {}

CatalogTestTool::CatalogTestTool(const std::string& name, const DirSpec& spec)
    : name_(name),
      spec_(spec),
      spooler_(),
      catalog_mgr_() {
  EXPECT_TRUE(InitDownloadManager(true));

  shash::Any root_hash(shash::kSha1);

  const std::string sandbox_root = GetCurrentWorkingDirectory();

  const std::string stratum0 = sandbox_root + "/" + name + "_stratum0";
  MkdirDeep(stratum0 + "/data", 0777);
  MakeCacheDirectories(stratum0 + "/data", 0777);
  const std::string temp_dir = stratum0 + "/data/txn";

  spooler_ = CreateSpooler("local," + temp_dir + "," + stratum0);

  catalog_mgr_ = CreateInputCatalogMgr(
      "file://" + stratum0, temp_dir, false, spooler_.weak_ref(),
      download_manager(), &stats_);

  catalog_mgr_->Init();

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
