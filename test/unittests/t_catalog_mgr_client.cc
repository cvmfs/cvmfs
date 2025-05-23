/**
 * This file is part of the CernVM File System.
 */

#include <alloca.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "c_file_sandbox.h"
#include "cache_posix.h"
#include "cache_tiered.h"
#include "catalog_mgr_client.h"
#include "catalog_mgr_rw.h"
#include "catalog_test_tools.h"
#include "crypto/signature.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "mountpoint.h"
#include "network/download.h"
#include "options.h"
#include "quota.h"
#include "testutil.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/uuid.h"

using namespace std;  // NOLINT

namespace catalog {

class T_CatalogManagerClient : public ::testing::Test {
 protected:
  virtual void SetUp() {
    repo_path_ = "repo";
    used_fds_ = GetNoUsedFds();
    fd_cwd_ = open(".", O_RDONLY);
    ASSERT_GE(fd_cwd_, 0);
    tmp_path_ = CreateTempDir("./cvmfs_ut_catalog_manager_client");
    options_mgr_.SetValue("CVMFS_CACHE_BASE", tmp_path_);
    options_mgr_.SetValue("CVMFS_SHARED_CACHE", "no");
    options_mgr_.SetValue("CVMFS_MAX_RETRIES", "0");
    fs_info_.name = "unit-test";
    fs_info_.options_mgr = &options_mgr_;
    // Silence syslog error
    options_mgr_.SetValue("CVMFS_MOUNT_DIR", "/no/such/dir");
  }

  virtual void TearDown() {
    const int retval = fchdir(fd_cwd_);
    ASSERT_EQ(0, retval);
    close(fd_cwd_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    if (repo_path_ != "")
      RemoveTree(repo_path_);
    // EXPECT_EQ(used_fds_, GetNoUsedFds()) << ShowOpenFiles();
    std::cerr << "FD counter test skipped" << std::endl;
  }

 protected:
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  string tmp_path_;
  string repo_path_;
  int fd_cwd_;
  unsigned used_fds_;
};

namespace {

struct fileSpec {
  const char *hash;
  size_t file_size;
};

fileSpec fileSpecs[] = {
    {.hash = "b026324c6904b2a9cb4b88d6d61c81d100000000",
     .file_size = 4096ul * 20},
    {.hash = "26ab0db90d72e28ad0ba1e22ee51051000000000",
     .file_size = 4096ul * 10},
    {.hash = "6d7fce9fee471194aa8b5b6e47267f0300000000",
     .file_size = 4096ul * 10},
    {.hash = "48a24b70a0b376535542b996af51739800000000",
     .file_size = 4096ul * 10},
    {.hash = "1dcca23355272056f04fe8bf20edfce000000000",
     .file_size = 4096ul * 10},
    {.hash = "1111111111111111111111111111111111111111",
     .file_size = 4096ul * 10},
    {.hash = "2222222222222222222222222222222222222222",
     .file_size = 4096ul * 10},
};

const size_t g_file_size = 4096;
}  // anonymous namespace

// Create directory specification for later repositories
DirSpec MakeCMCBaseSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", g_file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(
      spec.AddFile("file1", "dir", fileSpecs[0].hash, fileSpecs[0].file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir2"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir3"));

  // adding "/file3"
  EXPECT_TRUE(
      spec.AddFile("file3", "", fileSpecs[2].hash, fileSpecs[2].file_size));

  // adding "/dir/dir/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir", fileSpecs[1].hash,
                           fileSpecs[1].file_size));

  // adding "/dir/dir2/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir2", fileSpecs[3].hash,
                           fileSpecs[3].file_size));


  // adding "/dir/dir3/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir3", fileSpecs[4].hash,
                           fileSpecs[4].file_size));
  EXPECT_TRUE(spec.AddFile("file3", "dir/dir3", fileSpecs[5].hash,
                           fileSpecs[5].file_size));
  EXPECT_TRUE(spec.AddFile("file4", "dir/dir3", fileSpecs[6].hash,
                           fileSpecs[6].file_size));

  // Adding Deeply nested catalog
  EXPECT_TRUE(spec.AddDirectory("dir", "dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir", "dir/dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddFile("file1", "dir/dir/dir/dir", fileSpecs[0].hash,
                           fileSpecs[0].file_size));

  return spec;
}

void CreateCMCMiniRepository(SimpleOptionsParser *options_mgr_,
                             string *repo_path_) {
  CatalogTestTool tester(*repo_path_);
  EXPECT_TRUE(tester.Init());
  *repo_path_ = tester.repo_name();

  // Create file structure
  const DirSpec spec1 = MakeCMCBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.UpdateManifest();

  options_mgr_->SetValue("CVMFS_ROOT_HASH",
                         tester.manifest()->catalog_hash().ToString());
  options_mgr_->SetValue("CVMFS_SERVER_URL", "file://" + *repo_path_);
  options_mgr_->SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  options_mgr_->SetValue("CVMFS_PUBLIC_KEY", tester.public_key());
  options_mgr_->SetValue("TEST_REPO_NAME",
                         tester.manifest()->repository_name());
}

/**
 * Tests provided
 *  - LoadByHash
 *      - Load catalogs by hash (root, nested, and the same cached nested)
 *  - LoadByHashNetworkFailure
 *      - Simulate network failure when trying to load a not-yet-loaded catalog
 *  - LoadRootCatalog
 *      - Load root catalog after mount
 *      - This will check all storage loactions (local, alien, remote) which
 *        has the newest version
 *      - As it is unchanged, it will return local has already the newest one
 *
 * The following tests are only provided as integration tests. Would be nice
 * to have them also tested here, but the test suite does not provide
 * the required functionality
 *
 * Tests only provided by integration test:
 *  - LoadNewRootCatalog
 *      - Have current root catalog, server gets update, load new root catalog
 *  - LoadNewRootCatalogAlienCache
 *      - Have current root catalog with attached alien cache
 *      - Alien cache gets new update
 *      - LoadCatalog should select the newer alien cache (and not server
 *        or local)
 *  - CacheEvictFilesForCatalog
 *      - Cache full with catalogs and files
 *      - Even though cache is full, the loading of a new catalog must be
 *        successful because
 *
 * No implementation provided
 *  - LoadByHash_CacheTooSmall
 *      - Idea: Loading catalogs fails because entire cache is too small for it
 *  - DryRun Check if new RootCatalog is available
 */


TEST_F(T_CatalogManagerClient, LoadByHash) {
  CreateCMCMiniRepository(&options_mgr_, &repo_path_);
  ASSERT_TRUE(HasSuffix(repo_path_, "repo", false));
  const UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  string root_hash_str;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash_str));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  const UniquePtr<MountPoint> mp(
      MountPoint::Create(options_mgr_.GetValueOrDie("TEST_REPO_NAME"),
                         fs.weak_ref(),
                         &options_mgr_));
  EXPECT_EQ(loader::kFailOk, mp->boot_status());
  EXPECT_EQ(root_hash_str, mp->catalog_mgr()->GetRootHash().ToString());

  // load root catalog by its hash
  const PathString rootMntpnt("");
  const shash::Any &rootHash = mp->catalog_mgr()->GetRootHash();

  // root catalog is already mounted
  CatalogContext root_info(rootHash, rootMntpnt, kCtlgLocationMounted);

  EXPECT_EQ(catalog::kLoadUp2Date,
            mp->catalog_mgr()->LoadCatalogByHash(&root_info));
  EXPECT_EQ(root_hash_str, root_info.hash().ToString());

  // load nested catalog
  const PathString nested_mntpnt("/dir/dir2");
  const shash::Any &n_catalog_hash = mp->catalog_mgr()->GetNestedCatalogHash(
      nested_mntpnt);
  CatalogContext n_ctlg_context(n_catalog_hash, nested_mntpnt);

  EXPECT_EQ(catalog::kLoadNew,
            mp->catalog_mgr()->LoadCatalogByHash(&n_ctlg_context));

  // also chached should return the same answer
  EXPECT_EQ(catalog::kLoadNew,
            mp->catalog_mgr()->LoadCatalogByHash(&n_ctlg_context));
}


TEST_F(T_CatalogManagerClient, LoadByHashNetworkFailure) {
  CreateCMCMiniRepository(&options_mgr_, &repo_path_);
  ASSERT_TRUE(HasSuffix(repo_path_, "repo", false));
  const UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  string root_hash_str;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash_str));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  const UniquePtr<MountPoint> mp(
      MountPoint::Create(options_mgr_.GetValueOrDie("TEST_REPO_NAME"),
                         fs.weak_ref(),
                         &options_mgr_));
  EXPECT_EQ(loader::kFailOk, mp->boot_status());
  EXPECT_EQ(root_hash_str, mp->catalog_mgr()->GetRootHash().ToString());

  // load nested catalog
  const PathString nested_mntpnt("/dir/dir2");
  const shash::Any &n_catalog_hash = mp->catalog_mgr()->GetNestedCatalogHash(
      nested_mntpnt);
  CatalogContext ctlg_context(n_catalog_hash, nested_mntpnt);

  EXPECT_EQ(catalog::kLoadNew,
            mp->catalog_mgr()->LoadCatalogByHash(&ctlg_context));

  // also chached should return the same answer
  EXPECT_EQ(catalog::kLoadNew,
            mp->catalog_mgr()->LoadCatalogByHash(&ctlg_context));

  // break URL to repo
  mp->download_mgr()->SetHostChain("file://noValidURL");
  mp->download_mgr()->SwitchHost();


  // fetch hash but do not load catalog
  const PathString nested_mntpntNoDwnld("/dir/dir3");
  const shash::Any &n_catalog_hashNoDwnld = mp->catalog_mgr()
                                                ->GetNestedCatalogHash(
                                                    nested_mntpntNoDwnld);

  // try to load from cache
  CatalogContext root_info;
  mp->catalog_mgr()->GetNewRootCatalogContext(&root_info);

  EXPECT_EQ(catalog::kLoadUp2Date,
            mp->catalog_mgr()->LoadCatalogByHash(&root_info));
  EXPECT_EQ(root_info.hash().ToString(), root_hash_str);

  EXPECT_EQ(catalog::kLoadNew,
            mp->catalog_mgr()->LoadCatalogByHash(&ctlg_context));

  // fail to load new unloaded nested catalog
  CatalogContext ctlg_context_failed(n_catalog_hashNoDwnld,
                                     nested_mntpntNoDwnld);
  EXPECT_EQ(catalog::kLoadFail,
            mp->catalog_mgr()->LoadCatalogByHash(&ctlg_context_failed));
}

TEST_F(T_CatalogManagerClient, LoadRootCatalog) {
  CreateCMCMiniRepository(&options_mgr_, &repo_path_);
  ASSERT_TRUE(HasSuffix(repo_path_, "repo", false));
  const UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  string root_hash_str;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash_str));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  const UniquePtr<MountPoint> mp(
      MountPoint::Create(options_mgr_.GetValueOrDie("TEST_REPO_NAME"),
                         fs.weak_ref(),
                         &options_mgr_));
  EXPECT_EQ(loader::kFailOk, mp->boot_status());
  EXPECT_EQ(root_hash_str, mp->catalog_mgr()->GetRootHash().ToString());

  // load new root catalog without providing the hash
  // this will perform a check vs storage location which has the most recent one
  CatalogContext root_info;

  EXPECT_EQ(catalog::kLoadUp2Date,
            mp->catalog_mgr()->GetNewRootCatalogContext(&root_info));
  EXPECT_EQ(catalog::kCtlgLocationMounted, root_info.root_ctlg_location());
  EXPECT_EQ(root_hash_str, root_info.hash().ToString());
}

}  // namespace catalog
