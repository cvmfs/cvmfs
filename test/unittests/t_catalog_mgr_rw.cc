/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "catalog_mgr_rw.h"
#include "catalog_test_tools.h"
#include "download.h"
#include "statistics.h"
#include "upload.h"

using namespace std;  // NOLINT

namespace {

// Create some default hashes for DirSpec
const char* g_hashes[] = {"b026324c6904b2a9cb4b88d6d61c81d100000000",
                          "26ab0db90d72e28ad0ba1e22ee51051000000000",
                          "6d7fce9fee471194aa8b5b6e47267f0300000000",
                          "48a24b70a0b376535542b996af51739800000000",
                          "1dcca23355272056f04fe8bf20edfce000000000",
                          "1111111111111111111111111111111111111111",
                          "a34b51ff1b544f7f8d14e0fa5141830f00000000",
                          "6521257477da480594743cb7b24535ff00000000",
};

const size_t g_file_size = 4096;

// Create directory specification for later repositories
DirSpec MakeBaseSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", g_file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(spec.AddFile("file1", "dir", g_hashes[0], g_file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", g_file_size));

  // adding "/file3"
  EXPECT_TRUE(spec.AddFile("file3", "", g_hashes[2], g_file_size));

  // adding "/dir/dir/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir", g_hashes[1], g_file_size));

  // adding "/dir/dir2/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir2", g_hashes[3], g_file_size));

  // adding "/dir/dir3/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir3", g_hashes[4], g_file_size));

  // Adding Deeply nested catalog
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir/dir", g_file_size));
  EXPECT_TRUE(
    spec.AddFile("file1",  "dir/dir/dir/dir", g_hashes[0], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir"));

  // adding sub-nested catalogs
  EXPECT_TRUE(spec.AddDirectory("sub1", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddFile("file1", "dir/dir/dir/sub1",
                           g_hashes[6], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/sub1"));
  EXPECT_TRUE(spec.AddDirectory("sub2", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir/dir/sub2",
                           g_hashes[7], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/sub2"));

  return spec;
}

}  // anonymous namespace


namespace catalog {

class T_CatalogMgrRw : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};



TEST_F(T_CatalogMgrRw, CloneTreeFailSlow) {
  CatalogTestTool tester("clone_tree_fail_slow");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();
  EXPECT_DEATH(catalog_mgr->CloneTree("", ""), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("", "clone"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir", ""), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir", "dir"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir", "dir/clone"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("void", "clone"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir/file1", "clone"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir", "void/clone"), ".*");
  EXPECT_DEATH(catalog_mgr->CloneTree("dir/dir", "dir/dir2"), ".*");
}


TEST_F(T_CatalogMgrRw, CloneTree) {
  CatalogTestTool tester("clone_tree");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();
  catalog_mgr->CloneTree("dir", "clone");

  DirectoryEntry dirent;
  EXPECT_TRUE(catalog_mgr->LookupPath("/clone/dir/dir/dir/file1",
                                      kLookupSole, &dirent));
  EXPECT_STREQ(g_hashes[0], dirent.checksum().ToString().c_str());
  EXPECT_EQ(g_file_size, dirent.size());

  EXPECT_TRUE(catalog_mgr->LookupPath("/clone/dir/dir",
                                      kLookupSole, &dirent));
  EXPECT_TRUE(dirent.IsNestedCatalogRoot());
}


TEST_F(T_CatalogMgrRw, SwapNestedCatalog) {
  CatalogTestTool tester("swap_nested_catalog");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  // Look up sub1 and sub2 nested catalogs
  PathString path;
  shash::Any sub1_hash;
  shash::Any sub2_hash;
  uint64_t sub1_size;
  uint64_t sub2_size;
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"),
                                        &path, &sub1_hash, &sub1_size));
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub2"),
                                        &path, &sub2_hash, &sub2_size));

  // Swap sub1 with itself
  DirectoryEntry dirent;
  catalog_mgr->DetachNested();
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", sub1_hash, sub1_size);
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub1/file1",
                                      kLookupSole, &dirent));
  EXPECT_STREQ(g_hashes[6], dirent.checksum().ToString().c_str());

  // Swap sub1 and sub2
  catalog_mgr->DetachNested();
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", sub2_hash, sub2_size);
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub2", sub1_hash, sub1_size);
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub1/file2",
                                      kLookupSole, &dirent));
  EXPECT_STREQ(g_hashes[7], dirent.checksum().ToString().c_str());
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub2/file1",
                                      kLookupSole, &dirent));
  EXPECT_STREQ(g_hashes[6], dirent.checksum().ToString().c_str());
}


TEST_F(T_CatalogMgrRw, SwapNestedCatalogFailSlow) {
  CatalogTestTool tester("swap_nested_catalog_fail_slow");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  // Look up sub1 and sub2 nested catalogs
  PathString path;
  shash::Any sub1_hash;
  shash::Any sub2_hash;
  uint64_t sub1_size;
  uint64_t sub2_size;
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"),
                                        &path, &sub1_hash, &sub1_size));
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub2"),
                                        &path, &sub2_hash, &sub2_size));

  // Create nonexistent nested catalog hash
  shash::Any subX_hash(shash::kMd5,
                       shash::HexPtr("3e25960a79dbc69b674cd4ec67a72c62"),
                       shash::kSuffixCatalog);
  uint64_t subX_size = 42;

  // Fail if parent catalog does not exist
  catalog_mgr->DetachNested();
  EXPECT_DEATH(catalog_mgr->SwapNestedCatalog("no/such/dir/sub1",
                                              sub1_hash, sub1_size),
               "could not find parent");

  // Fail for directory that is not a nested catalog
  catalog_mgr->DetachNested();
  EXPECT_DEATH(catalog_mgr->SwapNestedCatalog("dir/dir",
                                              sub1_hash, sub1_size),
               "not found in parent");

  // Fail for nested catalog that is already attached
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"),
                                        &path, &sub1_hash, &sub1_size));
  EXPECT_DEATH(catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1",
                                              sub1_hash, sub1_size),
               "already attached");

  // Fail for non-existent catalog
  catalog_mgr->DetachNested();
  EXPECT_DEATH(catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1",
                                              subX_hash, subX_size),
               "failed to load");
}

}  // namespace catalog
