/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include "../../cvmfs/catalog.h"
#include "../../cvmfs/catalog_mgr.h"
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
  }

  void TearDown() {
    delete catalog_mgr_;
  }

  void AddTree() {
    const shash::Any empty_hash = shash::Any();
    const int size = 32;
    const size_t file_size = 4096;
    const char suffix = shash::kSha1;
    ASSERT_TRUE(catalog_mgr_->Init());
    shash::Any hash;
    shash::Any empty_content;
    // adding ""
    MockCatalog *root_catalog = catalog_mgr_->RetrieveRootCatalog();
    root_catalog->AddFile(empty_content, file_size, "", "");
    // adding "/dir"
    root_catalog->AddFile(empty_content, file_size, "", "dir");
    // adding "/file1"
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[1]),
                      size, suffix);
    root_catalog->AddFile(hash, file_size, "", "file1");
    // adding "/dir/dir"
    root_catalog->AddFile(empty_content, file_size, "/dir", "dir");
    // adding "/dir/dir/file2"
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[1]),
                      size, suffix);
    root_catalog->AddFile(hash, file_size, "/dir/dir", "file1");
    // adding "/dir/dir/dir"
    root_catalog->AddFile(empty_content, file_size, "/dir/dir", "dir");
    // adding a nested catalog in "/dir/dir/dir"
    string catalog_path_str = "/dir/dir/dir";
    PathString catalog_path(catalog_path_str.c_str(),
                            catalog_path_str.length());
    MockCatalog *new_catalog = catalog_mgr_->CreateCatalog(
        catalog_path, shash::Any(), root_catalog);
    ASSERT_NE(static_cast<MockCatalog*>(NULL), new_catalog);
    ASSERT_EQ(1u, root_catalog->GetChildren().size());
    // adding "/dir/dir/dir/file3" to the new nested catalog
    hash = shash::Any(shash::kSha1,
                          reinterpret_cast<const unsigned char*>(hashes[2]),
                          size, suffix);
    new_catalog->AddFile(hash, file_size, "/dir/dir/dir", "file3");
    // adding "/dir/dir/dir/file4" to the new nested catalog
    hash = shash::Any(shash::kSha1,
                          reinterpret_cast<const unsigned char*>(hashes[3]),
                          size, suffix);
    new_catalog->AddFile(hash, file_size, "/dir/dir/dir", "file4");
    root_catalog->RemoveChild(new_catalog);
    ASSERT_EQ(0u, root_catalog->GetChildren().size());
    ASSERT_EQ(1, catalog_mgr_->GetNumCatalogs());
  }

 protected:
  const static char *hashes[];
  MockCatalogManager *catalog_mgr_;
  perf::Statistics statistics_;
};

const char *T_CatalogManager::hashes[] = {
     "b026324c6904b2a9cb4b88d6d61c81d1000000",
     "26ab0db90d72e28ad0ba1e22ee510510000000",
     "6d7fce9fee471194aa8b5b6e47267f03000000",
     "48a24b70a0b376535542b996af517398000000",
     "1dcca23355272056f04fe8bf20edfce0000000"
};



TEST_F(T_CatalogManager, InitialConfiguration) {
  EXPECT_TRUE(catalog_mgr_->Init());
  EXPECT_EQ(1, catalog_mgr_->GetNumCatalogs());
  EXPECT_EQ(1u, catalog_mgr_->GetRevision());
  EXPECT_FALSE(catalog_mgr_->GetVolatileFlag());
  EXPECT_EQ(0u, catalog_mgr_->GetTTL());
}

TEST_F(T_CatalogManager, Statistics) {
  EXPECT_TRUE(catalog_mgr_->Init());
  Statistics st = catalog_mgr_->statistics();
  EXPECT_EQ(0u, st.n_listing->Get());
  EXPECT_EQ(0u, st.n_lookup_inode->Get());
  EXPECT_EQ(0u, st.n_lookup_path->Get());
  EXPECT_EQ(0u, st.n_lookup_path_negative->Get());
  EXPECT_EQ(0u, st.n_lookup_xattrs->Get());
  EXPECT_EQ(0u, st.n_nested_listing->Get());
}

TEST_F(T_CatalogManager, InodeConfiguration) {
  InodeGenerationAnnotation annotation;
  catalog_mgr_->SetInodeAnnotation(&annotation);
  EXPECT_TRUE(catalog_mgr_->Init());
}

TEST_F(T_CatalogManager, Lookup) {
  AddTree();
  catalog::DirectoryEntry dirent;
  LookupOptions option = kLookupSole;
  EXPECT_TRUE(catalog_mgr_->LookupPath("/dir", option, &dirent));
}

TEST_F(T_CatalogManager, Remount) {
  EXPECT_TRUE(catalog_mgr_->Init());
  EXPECT_EQ(kLoadNew, catalog_mgr_->Remount(true));
}

}
