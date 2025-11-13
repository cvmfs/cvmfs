/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include "mountpoint.h"
#include "util/uuid.h"
#include "testutil.h"
#include "options.h"
#include "bundle_mgr.h"

using namespace std;  // NOLINT

class T_BundleMgr: public ::testing::Test {
 protected:
  virtual void SetUp() {
    repo_path_ = "repo";
    uuid_dummy_ = cvmfs::Uuid::Create("");
    used_fds_ = GetNoUsedFds();
    fd_cwd_ = open(".", O_RDONLY);
    ASSERT_GE(fd_cwd_, 0);
    tmp_path_ = CreateTempDir("./cvmfs_ut_cache");
    options_mgr_.SetValue("CVMFS_CACHE_BASE", tmp_path_);
    options_mgr_.SetValue("CVMFS_SHARED_CACHE", "no");
    options_mgr_.SetValue("CVMFS_MAX_RETRIES", "0");
    fs_info_.name = "unit-test";
    fs_info_.options_mgr = &options_mgr_;
    // Silence syslog error
    options_mgr_.SetValue("CVMFS_MOUNT_DIR", "/no/such/dir");
    file_system_ = FileSystem::Create(fs_info_);
    mount_point_ = MountPoint::Create("keys.cern.ch", file_system_);
  }

  virtual void TearDown() {
    delete uuid_dummy_;
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
  MountPoint *mount_point_;
  FileSystem *file_system_;
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  string tmp_path_;
  string repo_path_;
  int fd_cwd_;
  unsigned used_fds_;
  /**
   * Initialize libuuid / open file descriptor on /dev/urandom
   */
  cvmfs::Uuid *uuid_dummy_;

  fuse_ino_t ino_;
};


TEST_F(T_BundleMgr, Construct) {
  BundleMgr b_mgr (mount_point_,ino_);
}

