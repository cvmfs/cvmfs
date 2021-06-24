/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "magic_xattr.h"
#include "mountpoint.h"
#include "options.h"
#include "testutil.h"
#include "util/posix.h"
#include "uuid.h"

class T_MagicXattr : public ::testing::Test {
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
    delete mount_point_;
    delete file_system_;
    delete uuid_dummy_;
    int retval = fchdir(fd_cwd_);
    ASSERT_EQ(0, retval);
    close(fd_cwd_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    if (repo_path_ != "")
      RemoveTree(repo_path_);
  }

  MountPoint *mount_point_;
  FileSystem *file_system_;

  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  std::string tmp_path_;
  std::string repo_path_;
  int fd_cwd_;
  unsigned used_fds_;
  /**
   * Initialize libuuid / open file descriptor on /dev/urandom
   */
  cvmfs::Uuid *uuid_dummy_;
};

TEST_F(T_MagicXattr, TestFqrn) {
  MagicXattrManager *mgr = new MagicXattrManager(mount_point_, false);

  catalog::DirectoryEntry dirent =
    catalog::DirectoryEntryTestFactory::ExternalFile();
  PathString path("/asdf");
  MagicXattrRAIIWrapper attr(mgr->GetLocked("user.fqrn", path, &dirent));
  ASSERT_FALSE(attr.IsNull());
  ASSERT_TRUE(attr->PrepareValueFenced());
  EXPECT_STREQ("keys.cern.ch", attr->GetValue().c_str());
}

TEST_F(T_MagicXattr, HideAttributes) {
  shash::Any hash;
  catalog::DirectoryEntry dirent =
    catalog::DirectoryEntryTestFactory::RegularFile("name", 42, hash);
  MagicXattrManager *mgr = new MagicXattrManager(mount_point_, true);
  std::string list = mgr->GetListString(&dirent);
  EXPECT_EQ(0U, list.length());
}
