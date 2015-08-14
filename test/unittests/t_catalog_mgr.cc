/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include "../../cvmfs/catalog.h"
#include "../../cvmfs/catalog_mgr.h"
#include "../../cvmfs/catalog_rw.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/shortstring.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT


namespace catalog {

class T_CatalogManager : public ::testing::Test {
 protected:
  void SetUp() {
    catalog_mgr_ = new MockCatalogManager(&statistics_);
    catalog_mgr_->Init();
  }

  void TearDown() {
    delete catalog_mgr_;
  }

 private:
  MockCatalogManager *catalog_mgr_;
  perf::Statistics statistics_;
};

TEST_F(T_CatalogManager, ) {

}

}
