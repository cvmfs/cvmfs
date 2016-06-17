/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "mountpoint.h"
#include "options.h"
#include "testutil.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "uuid.h"

using namespace std;  // NOLINT

class T_MountPoint : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    fd_cwd_ = open(".", O_RDONLY);
    ASSERT_GE(fd_cwd_, 0);
    tmp_path_ = CreateTempDir("./cvmfs_ut_cache");
    options_mgr_.SetValue("CVMFS_CACHE_BASE", tmp_path_);
    options_mgr_.SetValue("CVMFS_SHARED_CACHE", "no");
    fs_info_.name = "unit-test";
    fs_info_.options_mgr = &options_mgr_;
  }

  virtual void TearDown() {
    int retval = fchdir(fd_cwd_);
    close(fd_cwd_);
    ASSERT_EQ(0, retval);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

 protected:
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  string tmp_path_;
  int fd_cwd_;
  unsigned used_fds_;
};


TEST_F(T_MountPoint, CreateBasic) {
  FileSystem *file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);
  EXPECT_TRUE(file_system->IsValid());
  delete file_system;

  // Cached
  file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);
  EXPECT_TRUE(file_system->IsValid());
  delete file_system;
}


TEST_F(T_MountPoint, CacheSettings) {
  options_mgr_.SetValue("CVMFS_ALIEN_CACHE", tmp_path_ + "/alien");
  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "-1");
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_SHARED_CACHE");
  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "10");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "-1");
  options_mgr_.SetValue("CVMFS_NFS_SOURCE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_NFS_SOURCE");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }

  fs_info_.type = FileSystem::kFsLibrary;
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_SHARED_CACHE");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ(tmp_path_ + "/unit-test", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/alien", fs->cache_dir());
  }

  fs_info_.type = FileSystem::kFsFuse;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/alien", fs->cache_dir());
  }

  RemoveTree(tmp_path_ + "/unit-test");
  options_mgr_.UnsetValue("CVMFS_ALIEN_CACHE");
  options_mgr_.SetValue("CVMFS_NFS_SOURCE", "yes");
  options_mgr_.SetValue("CVMFS_NFS_SHARED", tmp_path_ + "/nfs");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_TRUE(fs->cache_mode() & FileSystem::kCacheNfs);
    EXPECT_TRUE(fs->cache_mode() & FileSystem::kCacheNfsHa);
    EXPECT_EQ(".", fs->cache_dir());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/nfs", fs->nfs_maps_dir());
  }
}


TEST_F(T_MountPoint, SharedDirectory) {
  // Same directory, different file system name
  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      fs_info_.name = "other";
      UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
      int retval = fs01->IsValid() ? 0 : 1;
      exit(retval);
  }

  UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
  EXPECT_TRUE(fs01->IsValid());
  int stat_loc;
  int retval = waitpid(pid, &stat_loc, 0);
  EXPECT_NE(retval, -1);
  EXPECT_TRUE(WIFEXITED(stat_loc));
  EXPECT_EQ(0, WEXITSTATUS(stat_loc));
}


TEST_F(T_MountPoint, CrashGuard) {
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_FALSE(fs->found_previous_crash());
  }

  MkdirDeep(tmp_path_ + "/unit-test", 0700, false);
  int fd = open((tmp_path_ + string("/unit-test/running.unit-test")).c_str(),
                O_RDWR | O_CREAT, 0600);
  ASSERT_GE(fd, 0);
  close(fd);

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  EXPECT_EQ(loader::kFailOk, fs->boot_status());
  EXPECT_TRUE(fs->found_previous_crash());
}


TEST_F(T_MountPoint, LockWorkspace) {
  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
      switch (fs01->boot_status()) {
        case loader::kFailOk:
          exit(0);
        case loader::kFailLockWorkspace:
          exit(1);
        default:
          exit(2);
      }
  }

  UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
  int stat_loc;
  int retval = waitpid(pid, &stat_loc, 0);
  EXPECT_NE(retval, -1);
  EXPECT_TRUE(WIFEXITED(stat_loc));
  EXPECT_TRUE(((fs01->boot_status() == 0) && (WEXITSTATUS(stat_loc) == 1)) ||
              ((fs01->boot_status() == loader::kFailLockWorkspace) &&
                  (WEXITSTATUS(stat_loc) == 0)));
}


TEST_F(T_MountPoint, UuidCache) {
  string cached_uuid;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    cached_uuid = fs->uuid_cache()->uuid();
  }
  fchdir(fd_cwd_);

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  EXPECT_EQ(cached_uuid, fs->uuid_cache()->uuid());
}


TEST_F(T_MountPoint, QuotaMgr) {
  // Fails because the unit test binary cannot become a quota manager process
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailQuota, fs->boot_status());
  }
}
