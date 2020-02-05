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
                          "1111111111111111111111111111111111111111"};

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

}  // namespace catalog
