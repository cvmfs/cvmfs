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
#include "util/string.h"
#include "util/uuid.h"

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
  std::set<std::string> protected_xattrs;
  std::set<gid_t> protected_xattr_gids;
  MagicXattrManager *mgr = new MagicXattrManager(
      mount_point_, MagicXattrManager::kVisibilityAlways, protected_xattrs,
      protected_xattr_gids);

  catalog::DirectoryEntry
      dirent = catalog::DirectoryEntryTestFactory::ExternalFile();
  PathString path("/asdf");
  MagicXattrRAIIWrapper attr(mgr->GetLocked("user.fqrn", path, &dirent));
  ASSERT_FALSE(attr.IsNull());
  ASSERT_TRUE(attr->PrepareValueFenced());
  EXPECT_STREQ("keys.cern.ch",
               attr->GetValue(0, kXattrMachineMode).second.c_str());
}

TEST_F(T_MagicXattr, TestLogBuffer) {
  std::set<std::string> protected_xattrs;
  std::set<gid_t> protected_xattr_gids;
  MagicXattrManager *mgr = new MagicXattrManager(
      mount_point_, MagicXattrManager::kVisibilityAlways, protected_xattrs,
      protected_xattr_gids);


  catalog::DirectoryEntry dirent;
  PathString path("/");

  LogCvmfs(kLogCvmfs, 0, "test");
  {
    MagicXattrRAIIWrapper attr(mgr->GetLocked("user.logbuffer", path, &dirent));
    ASSERT_FALSE(attr.IsNull());
    ASSERT_TRUE(attr->PrepareValueFenced());
    EXPECT_TRUE(HasSuffix(attr->GetValue(0, kXattrMachineMode).second, "test\n",
                          false /* ign_case */));
  }

  LogCvmfs(kLogCvmfs, 0, "%s", std::string(6000, 'x').c_str());
  {
    MagicXattrRAIIWrapper attr(mgr->GetLocked("user.logbuffer", path, &dirent));
    ASSERT_FALSE(attr.IsNull());
    ASSERT_TRUE(attr->PrepareValueFenced());
    EXPECT_TRUE(HasSuffix(attr->GetValue(0, kXattrMachineMode).second,
                          "<snip>\n", false /* ign_case */));
  }
}

TEST_F(T_MagicXattr, HideAttributes) {
  std::set<std::string> protected_xattrs;
  std::set<gid_t> protected_xattr_gids;
  catalog::DirectoryEntry
      dirent_name = catalog::DirectoryEntryTestFactory::RegularFile(
          "name", 42, shash::Any());
  catalog::DirectoryEntry
      dirent_root = catalog::DirectoryEntryTestFactory::Directory();

  MagicXattrManager *mgr_never = new MagicXattrManager(
      mount_point_, MagicXattrManager::kVisibilityNever, protected_xattrs,
      protected_xattr_gids);
  std::string list = mgr_never->GetListString(&dirent_name);
  EXPECT_EQ(0U, list.length());
  list = mgr_never->GetListString(&dirent_root);
  EXPECT_EQ(0U, list.length());

  MagicXattrManager *mgr_rootonly = new MagicXattrManager(
      mount_point_, MagicXattrManager::kVisibilityRootOnly, protected_xattrs,
      protected_xattr_gids);
  list = mgr_rootonly->GetListString(&dirent_name);
  EXPECT_EQ(0U, list.length());
  list = mgr_rootonly->GetListString(&dirent_root);
  EXPECT_LT(0U, list.length());
}

TEST_F(T_MagicXattr, ProtectedXattr) {
  std::set<std::string> protected_xattrs;
  protected_xattrs.insert("user.fqrn");

  std::set<gid_t> protected_xattr_gids;
  protected_xattr_gids.insert(1);

  MagicXattrManager *mgr = new MagicXattrManager(
      mount_point_, MagicXattrManager::kVisibilityAlways, protected_xattrs,
      protected_xattr_gids);
  mgr->Freeze();

  catalog::DirectoryEntry
      dirent = catalog::DirectoryEntryTestFactory::ExternalFile();
  PathString path("/asdf");
  MagicXattrRAIIWrapper attr(mgr->GetLocked("user.fqrn", path, &dirent));

  ASSERT_FALSE(attr.IsNull());
  ASSERT_FALSE(attr->PrepareValueFencedProtected(2));
  ASSERT_TRUE(attr->PrepareValueFencedProtected(1));
  EXPECT_STREQ("keys.cern.ch",
               attr->GetValue(0, kXattrMachineMode).second.c_str());
}

TEST_F(T_MagicXattr, MultiPageMachineModeXattr) {
  PubkeysMagicXattr attr;

  EXPECT_FALSE(attr.GetValue(0, kXattrMachineMode).first);

  attr.pubkeys_.push_back("xx");

  EXPECT_STREQ(attr.GetValue(0, kXattrMachineMode).second.c_str(), "xx");

  attr.pubkeys_.clear();
  attr.pubkeys_.push_back(std::string(10000, 'a'));
  attr.pubkeys_.push_back(std::string(10000, 'b'));
  attr.pubkeys_.push_back(std::string(10000, 'c'));
  attr.pubkeys_.push_back(std::string(10000, 'd'));
  attr.pubkeys_.push_back(std::string(10000, 'e'));
  attr.pubkeys_.push_back(std::string(10000, 'f'));

  EXPECT_EQ(attr.GetValue(0, kXattrMachineMode).second.find("ddddddd"),
            std::string::npos);
  EXPECT_GE((int)attr.GetValue(0, kXattrMachineMode).second.find("aaaaaa"), 0);
  EXPECT_GE((int)attr.GetValue(0, kXattrMachineMode).second.find("bbbbbb"), 0);

  EXPECT_EQ(attr.GetValue(1, kXattrMachineMode).second.find("ccccc"),
            std::string::npos);
  EXPECT_GE((int)attr.GetValue(1, kXattrMachineMode).second.find("dddddd"), 0);
  EXPECT_GE((int)attr.GetValue(1, kXattrMachineMode).second.find("fffffff"), 0);

  EXPECT_FALSE(attr.GetValue(3, kXattrMachineMode).first);
  EXPECT_EQ(
      attr.GetValue(1, kXattrMachineMode).second.find("# Access page at idx: "),
      std::string::npos);
}

TEST_F(T_MagicXattr, MultiPageHumanModeXattr) {
  PubkeysMagicXattr attr;

  EXPECT_EQ((int)attr.GetValue(0, kXattrHumanMode)
                .second.find("Page requested does not exists."),
            0);

  attr.pubkeys_.push_back(std::string(10000, 'a'));
  attr.pubkeys_.push_back(std::string(10000, 'b'));
  attr.pubkeys_.push_back(std::string(10000, 'c'));
  attr.pubkeys_.push_back(std::string(10000, 'd'));
  attr.pubkeys_.push_back(std::string(10000, 'e'));
  attr.pubkeys_.push_back(std::string(10000, 'f'));

  EXPECT_EQ((int)attr.GetValue(1, kXattrHumanMode)
                .second.find("# Access page at idx: "),
            0);
  EXPECT_EQ(attr.GetValue(1, kXattrHumanMode).second.find("ccccc"),
            std::string::npos);
  EXPECT_GE((int)attr.GetValue(1, kXattrHumanMode).second.find("ddddddd"), 0);
  EXPECT_GE((int)attr.GetValue(1, kXattrHumanMode).second.find("fffffff"), 0);


  EXPECT_EQ((int)attr.GetValue(3, kXattrHumanMode)
                .second.find("Page requested does not exists."),
            0);
}
