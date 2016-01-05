/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include "../../cvmfs/catalog.h"
#include "../../cvmfs/catalog_balancer.h"
#include "../../cvmfs/catalog_mgr.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/shortstring.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT


namespace catalog {

class T_CatalogManager : public ::testing::Test {
 public:
  T_CatalogManager() : statistics_(), catalog_mgr_(&statistics_) { }

 protected:
  /**
   * This method creates a file system structure in the catalogs as follows:
   *
   * +-/ (ROOT CATALOG)
   *   |
   *   +-file1
   *   |
   *   +-dir
   *     |
   *     +-dir
   *       |
   *       +-file2
   *       |
   *       +-dir (NESTED CATALOG)
   *         |
   *         +-file3
   *         |
   *         +-file4
   *         |
   *         +-dir
   *           |
   *           +-dir (NESTED CATALOG)
   *             |
   *             +-file5
   *
   */
  void AddTree() {
    const size_t file_size = 4096;
    char suffix = shash::kSha1;
    shash::Any hash;
    shash::Any empty_content;
    // adding ""
    MockCatalog *root_catalog = catalog_mgr_.RetrieveRootCatalog();
    root_catalog->AddFile(empty_content, file_size, "", "");
    // adding "/dir"
    root_catalog->AddFile(empty_content, file_size, "", "dir");
    // adding "/file1"
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[0]),
                      suffix);
    root_catalog->AddFile(hash, file_size, "", "file1");
    // adding "/dir/dir"
    root_catalog->AddFile(empty_content, file_size, "/dir", "dir");
    // adding "/dir/dir/file2"
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[1]),
                      suffix);
    root_catalog->AddFile(hash, file_size, "/dir/dir", "file2");
    // adding "/dir/dir/dir"
    root_catalog->AddFile(empty_content, file_size, "/dir/dir", "dir");
    // adding a nested catalog in "/dir/dir/dir"
    string catalog_path_str = "/dir/dir/dir";
    PathString catalog_path(catalog_path_str.c_str(),
                            catalog_path_str.length());
    MockCatalog *new_catalog = new MockCatalog("/dir/dir/dir", shash::Any(),
                                               4096, 1, 0, false,
                                               root_catalog, NULL);
    ASSERT_NE(static_cast<MockCatalog*>(NULL), new_catalog);
    // adding "/dir/dir/dir/file3" to the new nested catalog
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[2]),
                      suffix);
    new_catalog->AddFile(hash, file_size, "/dir/dir/dir", "file3");
    // adding "/dir/dir/dir/file4" to the new nested catalog
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[3]),
                      suffix);
    new_catalog->AddFile(hash, file_size, "/dir/dir/dir", "file4");
    // adding "/dir/dir/dir/dir"
    new_catalog->AddFile(empty_content, file_size, "/dir/dir/dir", "dir");
    // adding "/dir/dir/dir/dir/dir"
    new_catalog->AddFile(empty_content, file_size, "/dir/dir/dir/dir", "dir");
    // we haven't mounted the second catalog yet!
    ASSERT_EQ(1, catalog_mgr_.GetNumCatalogs());

    // mounting a new catalog in "/dir/dir/dir/dir/dir"
    MockCatalog *new_catalog_2 = new MockCatalog("/dir/dir/dir/dir/dir",
                                                 shash::Any(),
                                                 4096, 1, 0, false,
                                                 new_catalog, NULL);
    // adding "/dir/dir/dir/dir/dir/file5"
    hash = shash::Any(shash::kSha1,
                      reinterpret_cast<const unsigned char*>(hashes[4]),
                      suffix);
    new_catalog_2->AddFile(hash, file_size, "/dir/dir/dir/dir/dir", "file5");
    // we haven't mounted the third catalog yet!
    ASSERT_EQ(1, catalog_mgr_.GetNumCatalogs());
    catalog_mgr_.RegisterNewCatalog(new_catalog);
    catalog_mgr_.RegisterNewCatalog(new_catalog_2);
  }

 protected:
  static const char *hashes[];
  perf::Statistics statistics_;
  MockCatalogManager catalog_mgr_;
};

const char *T_CatalogManager::hashes[] = {
     "b026324c6904b2a9cb4b88d6d61c81d1000000",
     "26ab0db90d72e28ad0ba1e22ee510510000000",
     "6d7fce9fee471194aa8b5b6e47267f03000000",
     "48a24b70a0b376535542b996af517398000000",
     "1dcca23355272056f04fe8bf20edfce0000000"
};



TEST_F(T_CatalogManager, InitialConfiguration) {
  EXPECT_TRUE(catalog_mgr_.Init());
  EXPECT_EQ(1, catalog_mgr_.GetNumCatalogs());
  EXPECT_EQ(1u, catalog_mgr_.GetRevision());
  EXPECT_FALSE(catalog_mgr_.GetVolatileFlag());
  EXPECT_EQ(0u, catalog_mgr_.GetTTL());
}

TEST_F(T_CatalogManager, Statistics) {
  EXPECT_TRUE(catalog_mgr_.Init());
  const Statistics &st = catalog_mgr_.statistics();
  EXPECT_EQ(0u, st.n_listing->Get());
  EXPECT_EQ(0u, st.n_lookup_inode->Get());
  EXPECT_EQ(0u, st.n_lookup_path->Get());
  EXPECT_EQ(0u, st.n_lookup_path_negative->Get());
  EXPECT_EQ(0u, st.n_lookup_xattrs->Get());
  EXPECT_EQ(0u, st.n_nested_listing->Get());
}

TEST_F(T_CatalogManager, InodeConfiguration) {
  const uint64_t initial_offset = 1000;
  InodeGenerationAnnotation annotation;
  annotation.IncGeneration(initial_offset);
  catalog_mgr_.SetInodeAnnotation(&annotation);
  EXPECT_TRUE(catalog_mgr_.Init());
  // now the root catalog is loaded and has 6 entries
  EXPECT_EQ(initial_offset + 1 +
            AbstractCatalogManager<MockCatalogManager>::kInodeOffset,
            catalog_mgr_.GetRootInode());
}

TEST_F(T_CatalogManager, Lookup) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  AddTree();
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir", kLookupSole, &dirent));
  EXPECT_TRUE(dirent.IsDirectory());
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir", kLookupSole, &dirent));
  EXPECT_TRUE(dirent.IsDirectory());
  EXPECT_TRUE(catalog_mgr_.LookupPath("/file1", kLookupSole, &dirent));
  EXPECT_TRUE(dirent.IsRegular());
  EXPECT_TRUE(catalog_mgr_.LookupPath("/file1", kLookupFull, &dirent));
  EXPECT_TRUE(dirent.IsRegular());
  // the father directory belongs to the catalog, so there is no problem
  EXPECT_TRUE(
    catalog_mgr_.LookupPath("/dir/dir/file2", kLookupFull, &dirent));
  EXPECT_TRUE(dirent.IsRegular());
  // /dir/dir/dir/file4 belongs to a catalog that is not mounted yet
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir/dir/file4", kLookupSole,
                                      &dirent));
  // the new catalog should be mounted now
  EXPECT_EQ(2, catalog_mgr_.GetNumCatalogs());

  // the father directory should also belong to the nested catalog
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir/dir/file4", kLookupFull,
                                      &dirent));
  // it is not a symplink, so it should crash
  EXPECT_DEATH(catalog_mgr_.LookupPath("/dir/dir/dir/file4", kLookupRawSymlink,
                                      &dirent), ".*");

  // load the next catalog
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir/dir/dir/dir/file5",
                                      kLookupFull, &dirent));
  // the new catalog should be mounted now
  EXPECT_EQ(3, catalog_mgr_.GetNumCatalogs());
}

TEST_F(T_CatalogManager, LongLookup) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  AddTree();
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir/dir/dir/dir/file5",
                                      kLookupFull, &dirent));
  EXPECT_TRUE(dirent.IsRegular());
  // we should have mounted two catalogs
  EXPECT_EQ(3, catalog_mgr_.GetNumCatalogs());
}

TEST_F(T_CatalogManager, Listing) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  AddTree();
  DirectoryEntryList del;
  EXPECT_FALSE(catalog_mgr_.Listing("/fakepath", &del));
  EXPECT_EQ(0u, del.size());
  del.clear();
  EXPECT_TRUE(catalog_mgr_.Listing("/dir/dir", &del));
  EXPECT_EQ(2u, del.size());
  // now it will have to mount the nested catalog
  del.clear();
  EXPECT_TRUE(catalog_mgr_.Listing("/dir/dir/dir", &del));
  EXPECT_EQ(3u, del.size());
  EXPECT_EQ(2, catalog_mgr_.GetNumCatalogs());
  // mounting the next nested catalog
  del.clear();
  EXPECT_TRUE(catalog_mgr_.Listing("/dir/dir/dir/dir/dir", &del));
  EXPECT_EQ(1u, del.size());
  EXPECT_EQ(3, catalog_mgr_.GetNumCatalogs());
}

TEST_F(T_CatalogManager, LongListing) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  AddTree();
  DirectoryEntryList del;
  // mounting the directly the deepest nested catalog
  EXPECT_TRUE(catalog_mgr_.Listing("/dir/dir/dir/dir/dir", &del));
  EXPECT_EQ(1u, del.size());
  EXPECT_EQ(3, catalog_mgr_.GetNumCatalogs());
}

TEST_F(T_CatalogManager, FailListing) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  AddTree();
  DirectoryEntryList del;
  // trying with a fake path inside the nested catalog
  EXPECT_EQ(1, catalog_mgr_.GetNumCatalogs());
  EXPECT_FALSE(catalog_mgr_.Listing("/dir/dir/dir/fakedir", &del));
  EXPECT_EQ(0u, del.size());
  // even though the listing failed it should have loaded the nested catalog
  EXPECT_EQ(2, catalog_mgr_.GetNumCatalogs());
  // trying now with the next nested catalog
  EXPECT_FALSE(catalog_mgr_.Listing("/dir/dir/dir/dir/dir/fakedir", &del));
  EXPECT_EQ(0u, del.size());
  EXPECT_EQ(3, catalog_mgr_.GetNumCatalogs());
}

TEST_F(T_CatalogManager, Balance) {
  catalog::DirectoryEntry dirent;
  ASSERT_TRUE(catalog_mgr_.Init());
  EXPECT_EQ(1, catalog_mgr_.GetNumCatalogs());
  AddTree();
  // load and mount the catalogs
  EXPECT_EQ(0u, catalog_mgr_.GetNumAutogeneratedCatalogs());
  CatalogBalancer<MockCatalogManager> balancer(&catalog_mgr_);

  // initial balancing parameters for a MockCatalogManager are:
  // maximum weight = 5: not used here
  // minimum weight = 1: not used here
  // balance weight = 3: This parameter will be used, we won't check the others
  // because here only the actual balancing process is tested
  SetLogVerbosity(kLogDiscrete);
  balancer.Balance(NULL);
  SetLogVerbosity(kLogNormal);

  // it should now create nested catalogs in:
  //    - /dir/dir
  // notice that we have only loaded the first catalog!
  EXPECT_EQ(1u, catalog_mgr_.GetNumAutogeneratedCatalogs());

  // load the other catalogs so that they can be removed
  EXPECT_TRUE(catalog_mgr_.LookupPath("/dir/dir/dir/dir/dir/file5",
                                      kLookupFull, &dirent));
}

TEST_F(T_CatalogManager, Remount) {
  EXPECT_TRUE(catalog_mgr_.Init());
  LoadError le;
  EXPECT_EQ(kLoadNew, le = catalog_mgr_.Remount(true));
  EXPECT_EQ(kLoadNew, catalog_mgr_.Remount(false));
}

}  // namespace catalog
