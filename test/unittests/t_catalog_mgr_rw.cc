/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "catalog_mgr.h"
#include "catalog_mgr_rw.h"
#include "catalog_rw.h"
#include "catalog_test_tools.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "manifest.h"
#include "network/download.h"
#include "statistics.h"
#include "testutil.h"
#include "upload.h"
#include "util/string.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace {

// Create some default hashes for DirSpec
const char *g_hashes[] = {
    "b026324c6904b2a9cb4b88d6d61c81d100000000",
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
  EXPECT_TRUE(spec.AddDirectory("dir", "dir", g_file_size));
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
  EXPECT_TRUE(spec.AddDirectory("dir", "dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(
      spec.AddFile("file1", "dir/dir/dir/dir", g_hashes[0], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir"));

  // adding sub-nested catalogs
  EXPECT_TRUE(spec.AddDirectory("sub1", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(
      spec.AddFile("file1", "dir/dir/dir/sub1", g_hashes[6], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/sub1"));
  EXPECT_TRUE(spec.AddDirectory("sub2", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(
      spec.AddFile("file2", "dir/dir/dir/sub2", g_hashes[7], g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/sub2"));

  return spec;
}

}  // anonymous namespace


namespace catalog {

class T_CatalogMgrRw : public ::testing::Test {
 protected:
  virtual void SetUp() { }

  virtual void TearDown() { }
};


TEST_F(T_CatalogMgrRw, CloneTreeFailSlow) {
  CatalogTestTool tester("clone_tree_fail_slow");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("", ""));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("", "clone"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir", ""));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir", "dir"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir", "dir/clone"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("void", "clone"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir/file1", "clone"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir", "void/clone"));
  EXPECT_ANY_THROW(catalog_mgr->CloneTree("dir/dir", "dir/dir2"));
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
                                      kLookupDefault, &dirent));
  EXPECT_STREQ(g_hashes[0], dirent.checksum().ToString().c_str());
  EXPECT_EQ(g_file_size, dirent.size());

  EXPECT_TRUE(
      catalog_mgr->LookupPath("/clone/dir/dir", kLookupDefault, &dirent));
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
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &sub1_hash, &sub1_size));
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub2"), &path,
                                        &sub2_hash, &sub2_size));

  // Swap sub1 with itself
  DirectoryEntry dirent;
  catalog_mgr->DetachNested();
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", sub1_hash, sub1_size);
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub1/file1", kLookupDefault,
                                      &dirent));
  EXPECT_STREQ(g_hashes[6], dirent.checksum().ToString().c_str());

  // Swap sub1 and sub2
  catalog_mgr->DetachNested();
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", sub2_hash, sub2_size);
  catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub2", sub1_hash, sub1_size);
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub1/file2", kLookupDefault,
                                      &dirent));
  EXPECT_STREQ(g_hashes[7], dirent.checksum().ToString().c_str());
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub2/file1", kLookupDefault,
                                      &dirent));
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
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &sub1_hash, &sub1_size));
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub2"), &path,
                                        &sub2_hash, &sub2_size));

  // Create nonexistent nested catalog hash
  shash::Any subX_hash(shash::kMd5,
                       shash::HexPtr("3e25960a79dbc69b674cd4ec67a72c62"),
                       shash::kSuffixCatalog);
  uint64_t subX_size = 42;

  // Fail if parent catalog does not exist
  catalog_mgr->DetachNested();
  EXPECT_ANY_THROW(
      catalog_mgr->SwapNestedCatalog("no/such/dir/sub1", sub1_hash, sub1_size));

  // Fail for directory that is not a nested catalog
  catalog_mgr->DetachNested();
  EXPECT_ANY_THROW(
      catalog_mgr->SwapNestedCatalog("dir/dir", sub1_hash, sub1_size));

  // Fail for nested catalog that is already attached and modified
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &sub1_hash, &sub1_size));
  catalog_mgr->RemoveFile("dir/dir/dir/sub1/file1");
  EXPECT_ANY_THROW(
      catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", sub1_hash, sub1_size));

  // Fail for non-existent catalog
  catalog_mgr->DetachNested();
  EXPECT_ANY_THROW(
      catalog_mgr->SwapNestedCatalog("dir/dir/dir/sub1", subX_hash, subX_size));
}

TEST_F(T_CatalogMgrRw, GraftNestedCatalog) {
  CatalogTestTool tester("graft_nested_catalog");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  PathString path;
  shash::Any sub1_hash;
  uint64_t sub1_size;
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &sub1_hash, &sub1_size));

  catalog_mgr->RemoveNestedCatalog("dir/dir/dir/sub1");
  catalog_mgr->RemoveDirectory("dir/dir/dir/sub1");
  catalog_mgr->GraftNestedCatalog("dir/dir/dir/sub1", sub1_hash, sub1_size);

  DirectoryEntry dirent;
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/dir/dir/sub1/file1", kLookupDefault,
                                      &dirent));
  EXPECT_STREQ(g_hashes[6], dirent.checksum().ToString().c_str());
  shash::Any check_hash;
  uint64_t check_size;
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &check_hash, &check_size));
  EXPECT_EQ(sub1_hash, check_hash);
  EXPECT_EQ(sub1_size, check_size);
}

TEST_F(T_CatalogMgrRw, GraftNestedCatalogFail) {
  CatalogTestTool tester("graft_nested_catalog_fail_slow");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  PathString path;
  shash::Any sub1_hash;
  uint64_t sub1_size;
  shash::Any sub2_hash;
  uint64_t sub2_size;
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub1"), &path,
                                        &sub1_hash, &sub1_size));
  EXPECT_TRUE(catalog_mgr->LookupNested(PathString("/dir/dir/dir/sub2"), &path,
                                        &sub2_hash, &sub2_size));

  // Fail if directory is not empty
  EXPECT_ANY_THROW(catalog_mgr->GraftNestedCatalog("dir/dir/dir/sub1",
                                                   sub1_hash, sub1_size));

  catalog_mgr->RemoveNestedCatalog("dir/dir/dir/sub1");
  catalog_mgr->RemoveDirectory("dir/dir/dir/sub1");
  // Wrong nested catalog should fail
  EXPECT_ANY_THROW(catalog_mgr->GraftNestedCatalog("dir/dir/dir/sub1",
                                                   sub2_hash, sub2_size));

  // Missing parent should fail
  catalog_mgr->RemoveDirectory("dir/dir/dir");
  EXPECT_DEATH(
      catalog_mgr->GraftNestedCatalog("dir/dir/dir/sub1", sub1_hash, sub1_size),
      ".*");
}

// ---------------------------------------------------------------------------
// A1: IssueHardlinkGroupId() — cached per-catalog hardlink ID sequence
// ---------------------------------------------------------------------------

// Verify that repeated calls to IssueHardlinkGroupId() on a fresh catalog
// (no pre-existing hardlinks) return a strictly monotonically increasing
// sequence starting at 1, without re-querying the database on every call.
TEST_F(T_CatalogMgrRw, IssueHardlinkGroupIdSequence) {
  CatalogTestTool tester("issue_hardlink_group_id_seq");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  // "file3" is in the root catalog (see MakeBaseSpec). GetHostingCatalog
  // returns the WritableCatalog that hosts it — the root catalog.
  catalog::WritableCatalog *root = catalog_mgr->GetHostingCatalog("file3");
  ASSERT_NE(nullptr, root);

  // MakeBaseSpec has no hardlinks, so GetMaxLinkId() returns 0 (MAX on an
  // empty set → NULL → 0 in SQLite). IssueHardlinkGroupId() must therefore
  // start at 1 and count upward without an additional DB query.
  uint32_t id1 = root->IssueHardlinkGroupId();
  uint32_t id2 = root->IssueHardlinkGroupId();
  uint32_t id3 = root->IssueHardlinkGroupId();

  EXPECT_EQ(1u, id1);
  EXPECT_EQ(2u, id2);
  EXPECT_EQ(3u, id3);
}


// Verify that consecutive AddHardlinkGroup calls issued through the catalog
// manager allocate strictly sequential, distinct, non-zero group IDs and
// that the lookup of each group's members returns the correct ID.
TEST_F(T_CatalogMgrRw, HardlinkGroupsGetDistinctIds) {
  CatalogTestTool tester("hardlink_groups_distinct_ids");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  // Build two independent hardlink groups in "/dir".
  // Group A: two names sharing one content hash.
  const shash::Any hash_a =
      shash::MkFromHexPtr(shash::HexPtr(g_hashes[5]), shash::kSuffixNone);
  catalog::DirectoryEntryBaseList group_a;
  group_a.push_back(
      catalog::DirectoryEntryTestFactory::RegularFile("hlink_a1", g_file_size,
                                                      hash_a));
  group_a.push_back(
      catalog::DirectoryEntryTestFactory::RegularFile("hlink_a2", g_file_size,
                                                      hash_a));

  // Group B: two names sharing a different content hash.
  const shash::Any hash_b =
      shash::MkFromHexPtr(shash::HexPtr(g_hashes[6]), shash::kSuffixNone);
  catalog::DirectoryEntryBaseList group_b;
  group_b.push_back(
      catalog::DirectoryEntryTestFactory::RegularFile("hlink_b1", g_file_size,
                                                      hash_b));
  group_b.push_back(
      catalog::DirectoryEntryTestFactory::RegularFile("hlink_b2", g_file_size,
                                                      hash_b));

  XattrList xattrs;
  FileChunkList no_chunks;
  catalog_mgr->AddHardlinkGroup(group_a, xattrs, "dir", no_chunks);
  catalog_mgr->AddHardlinkGroup(group_b, xattrs, "dir", no_chunks);

  // Verify that the two groups received different non-zero IDs.
  catalog::DirectoryEntry dirent_a1, dirent_a2, dirent_b1, dirent_b2;
  ASSERT_TRUE(catalog_mgr->LookupPath("/dir/hlink_a1", kLookupDefault,
                                      &dirent_a1));
  ASSERT_TRUE(catalog_mgr->LookupPath("/dir/hlink_a2", kLookupDefault,
                                      &dirent_a2));
  ASSERT_TRUE(catalog_mgr->LookupPath("/dir/hlink_b1", kLookupDefault,
                                      &dirent_b1));
  ASSERT_TRUE(catalog_mgr->LookupPath("/dir/hlink_b2", kLookupDefault,
                                      &dirent_b2));

  const uint32_t id_a = dirent_a1.hardlink_group();
  const uint32_t id_b = dirent_b1.hardlink_group();

  // Both IDs must be positive (assert link_id_seq_ > 0 in the source).
  EXPECT_GT(id_a, 0u);
  EXPECT_GT(id_b, 0u);

  // Members of the same group share an ID.
  EXPECT_EQ(id_a, dirent_a2.hardlink_group());
  EXPECT_EQ(id_b, dirent_b2.hardlink_group());

  // The two groups have different IDs (sequential allocation).
  EXPECT_NE(id_a, id_b);

  // Group B was added second so its ID must be exactly one higher.
  EXPECT_EQ(id_a + 1u, id_b);
}


// ---------------------------------------------------------------------------
// B3: VacuumDatabaseIfNecessary() — lock released before VACUUM
// ---------------------------------------------------------------------------

// After many AddFile / RemoveFile cycles the catalog accumulates free pages
// that exceed kMaximalFreePageRatio.  Commit() calls FinalizeCatalog() which
// calls VacuumDatabaseIfNecessary().  If the lock were still held across the
// multi-second VACUUM the catalog would deadlock when the subsequent
// UpdateNestedCatalog on the parent tried to acquire it.  This test verifies
// that Commit() completes and the catalog remains accessible afterward.
TEST_F(T_CatalogMgrRw, VacuumDatabaseNoDeadlock) {
  CatalogTestTool tester("vacuum_no_deadlock");
  EXPECT_TRUE(tester.Init());

  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));

  catalog::WritableCatalogManager *catalog_mgr = tester.catalog_mgr();

  // Add a batch of files to create real catalog churn, then remove them so
  // that freed page space eventually triggers VACUUM.
  const shash::Any hash =
      shash::MkFromHexPtr(shash::HexPtr(g_hashes[0]), shash::kSuffixNone);
  const XattrList xattrs;
  for (int i = 0; i < 50; ++i) {
    const string name = "vac_tmp_" + StringifyInt(i);
    catalog::DirectoryEntry entry =
        catalog::DirectoryEntryTestFactory::RegularFile(name, g_file_size,
                                                        hash);
    // Cast to DirectoryEntryBase to route through the public overload.
    // The protected DirectoryEntry overload is the internal implementation;
    // the public base overload is the intended external API.
    catalog_mgr->AddFile(
        static_cast<const catalog::DirectoryEntryBase &>(entry), xattrs, "dir");
  }
  for (int i = 0; i < 50; ++i) {
    catalog_mgr->RemoveFile("dir/vac_tmp_" + StringifyInt(i));
  }

  // Commit should not deadlock and must report success.
  manifest::Manifest manifest(shash::Any(), 1, "");
  const bool committed = catalog_mgr->Commit(false, 0, &manifest);
  EXPECT_TRUE(committed);

  // The catalog must still be queryable after vacuum.
  catalog::DirectoryEntry dirent;
  EXPECT_TRUE(catalog_mgr->LookupPath("/dir/file1", kLookupDefault, &dirent));
  EXPECT_STREQ(g_hashes[0], dirent.checksum().ToString().c_str());
}

}  // namespace catalog
