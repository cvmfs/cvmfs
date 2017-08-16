/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include "catalog.h"
#include "catalog_rw.h"
#include "receiver/params.h"
#include "testutil.h"
#include "util/posix.h"

DirSpec::DirSpec(const DirSpecEntryList& entries) : entries_(entries) {}

DirSpec::~DirSpec() {}

CatalogTestTool::CatalogTestTool(const DirSpec& spec)
    : spec_(spec),
      statistics_(),
      catalog_mgr_(new catalog::MockCatalogManager(&statistics_)) {
  catalog_mgr_->Init();

  MockCatalog* root_catalog = catalog_mgr_->RetrieveRootCatalog();
  for (size_t i = 0; i < spec_.entries_.size(); ++i) {
    const DirSpecEntry& entry = spec_.entries_[i];
    root_catalog->AddFile(entry.hash_, entry.size_, entry.parent_,
                         entry.name_);
  }
}

CatalogTestTool::~CatalogTestTool() {}

