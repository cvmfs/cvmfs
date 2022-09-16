/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/param.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <alloca.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>


#include "cache_posix.h"
#include "cache_tiered.h"
#include "catalog_mgr_client.h"
#include "catalog_mgr_rw.h"
#include "catalog_test_tools.h"
#include "crypto/signature.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "mountpoint.h"
#include "options.h"
#include "testutil.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/uuid.h"

using namespace std;  // NOLINT

#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

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
    int retval = fchdir(fd_cwd_);
    ASSERT_EQ(0, retval);
    close(fd_cwd_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    if (repo_path_ != "")
      RemoveTree(repo_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds()) << ShowOpenFiles();
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
// Create some default hashes for DirSpec
const char* g_hashes[] = {"b026324c6904b2a9cb4b88d6d61c81d100000000",
                          "26ab0db90d72e28ad0ba1e22ee51051000000000",
                          "6d7fce9fee471194aa8b5b6e47267f0300000000",
                          "48a24b70a0b376535542b996af51739800000000",
                          "1dcca23355272056f04fe8bf20edfce000000000",
                          "1111111111111111111111111111111111111111"};

const size_t g_file_size = 4096;
}  // anonymous namespace

// Create directory specification for later repositories
DirSpec MakeCMCBaseSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", g_file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(spec.AddFile("file1", "dir", g_hashes[0], g_file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", g_file_size));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir2"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir3"));

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

  GTEST_COUT << "nested catalogs in dirspec" << std::endl;

  for(const auto& catalog : spec.nested_catalogs() ) {
    GTEST_COUT << catalog.c_str() << std::endl;
  }

  return spec;
}

void CreateCMCMiniRepository(
  SimpleOptionsParser *options_mgr_,
  string *repo_path_
) {
  CatalogTestTool tester(*repo_path_);
  EXPECT_TRUE(tester.Init());
  *repo_path_ = tester.repo_name();

  // Create file structure
  DirSpec spec1 = MakeCMCBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.UpdateManifest();

  GTEST_COUT << "Server Manifest\n" << tester.manifest()->ExportString() << std::endl;
  GTEST_COUT << "Root hash " <<  tester.manifest()->catalog_hash().ToString() << std::endl;
  GTEST_COUT << "Revision " << tester.manifest()->revision() << std::endl;

  

  options_mgr_->SetValue("CVMFS_ROOT_HASH",
                        tester.manifest()->catalog_hash().ToString());
  options_mgr_->SetValue("CVMFS_SERVER_URL", "file://" + *repo_path_);
  options_mgr_->SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  options_mgr_->SetValue("CVMFS_PUBLIC_KEY", tester.public_key());
  options_mgr_->SetValue("TEST_REPO_NAME", tester.manifest()->repository_name());
}

TEST_F(T_CatalogManagerClient, MountLatest) {
  CreateCMCMiniRepository(&options_mgr_, &repo_path_);
  ASSERT_TRUE(HasSuffix(repo_path_, "repo", false));
  GTEST_COUT << repo_path_ << std::endl;

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  string root_hash;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  GTEST_COUT << options_mgr_.GetValueOrDie("TEST_REPO_NAME") << std::endl;


  UniquePtr<MountPoint> mp(MountPoint::Create(options_mgr_.GetValueOrDie("TEST_REPO_NAME"), fs.weak_ref(), &options_mgr_));
  EXPECT_EQ(loader::kFailOk, mp->boot_status());

  GTEST_COUT << "Mountpoint Manifest\n" << mp->catalog_mgr()->manifest()->ExportString() << std::endl;

  mp->catalog_mgr()->Remount(false);

  GTEST_COUT << "Mountpoint Manifest - Remount\n" << mp->catalog_mgr()->manifest()->ExportString() << std::endl;

  EXPECT_EQ(root_hash, mp->catalog_mgr()->GetRootHash().ToString());
  EXPECT_TRUE(fs->cache_mgr()->LoadBreadcrumb(options_mgr_.GetValueOrDie("TEST_REPO_NAME")).IsValid());

  GTEST_COUT << "NumCatalogs " << mp->catalog_mgr()->GetNumCatalogs() << std::endl;

  const PathString rootpath("/");
  const auto& rootcatalog = mp->catalog_mgr()->FindCatalog(rootpath);
  GTEST_COUT << "Root catalog hash " << rootcatalog->hash().ToString() << std::endl;

  const PathString npath("/dir/dir2");
  const auto& ncatalogHash = mp->catalog_mgr()->GetNestedCatalogHash(npath);
  GTEST_COUT << "GetNestedCatalogHash for " << npath.c_str() << ": "  << ncatalogHash.ToString() << std::endl;
  const PathString npathx("/dir/dir2/file2");
  const auto& ncatalog = mp->catalog_mgr()->FindCatalog(npathx);
  GTEST_COUT << "FindCatalog for " << npathx.c_str() << ": "  << ncatalog->hash().ToString() << std::endl;
  // GTEST_COUT << "Parent catalog hash (root) " << ncatalog->parent()->hash().ToString() << std::endl;

  GTEST_COUT << "NumCatalogs " << mp->catalog_mgr()->GetNumCatalogs() << std::endl;

  std::string  catalog_path = "";
  shash::Any catalog_hash = shash::Any();
  const PathString  mountpoint("/dir/dir2");
  mp->catalog_mgr()->LoadCatalog(mountpoint, ncatalogHash, &catalog_path, &catalog_hash);

  GTEST_COUT << "After load catalog: " << catalog_path << " " << catalog_hash.ToString() << std::endl;


  const PathString npath2("/dir/dir3/file2");
  const auto& ncatalog2 = mp->catalog_mgr()->FindCatalog(npath2);
  GTEST_COUT << "Nested catalog 2 hash " << ncatalog2->hash().ToString() << std::endl;
  // GTEST_COUT << "Parent catalog 2 hash (root) " << ncatalog2->parent()->hash().ToString() << std::endl;

  GTEST_COUT << "NumCatalogs " << mp->catalog_mgr()->GetNumCatalogs() << std::endl;

  

  auto& catalogs = mp->catalog_mgr()->loaded_catalogs_;

  GTEST_COUT << "List catalogs: " << catalogs.size() << std::endl;
  for(const auto& catalog : catalogs) {
    GTEST_COUT << catalog.first.c_str() << " " << catalog.second.ToString() << std::endl;
  }

  GTEST_COUT << "List all catalogs of the repo directly under root" << std::endl;
  const auto& allCatalogs = mp->catalog_mgr()->GetRootCatalog()->ListNestedCatalogs();
  GTEST_COUT << "ROOT: " << mp->catalog_mgr()->GetRootCatalog()->hash().ToString() << " " << mp->catalog_mgr()->GetRootCatalog()->mountpoint().c_str() << std::endl;
  for(const auto catalog : allCatalogs) {
    // GTEST_COUT << catalog->hash().ToString() << " " << catalog->mountpoint().c_str() << std::endl;
    GTEST_COUT << catalog.hash.ToString() << " " << catalog.mountpoint.c_str() << std::endl;
  }


  GTEST_COUT << "FINISHED Function - MountLatest" << std::endl;


}

}