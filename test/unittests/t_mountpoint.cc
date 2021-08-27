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

#include "cache_posix.h"
#include "cache_tiered.h"
#include "catalog_mgr_client.h"
#include "catalog_mgr_rw.h"
#include "catalog_test_tools.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "mountpoint.h"
#include "options.h"
#include "signature.h"
#include "testutil.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "uuid.h"

using namespace std;  // NOLINT

class T_MountPoint : public ::testing::Test {
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


TEST_F(T_MountPoint, MkCacheParm) {
  FileSystem *file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);

  file_system->cache_mgr_instance_ = "ceph";
  EXPECT_EQ("CVMFS_CACHE_ceph_TYPE",
            file_system->MkCacheParm("CVMFS_CACHE_TYPE", "ceph"));
  file_system->cache_mgr_instance_ = file_system->kDefaultCacheMgrInstance;
  EXPECT_EQ("CVMFS_CACHE_TYPE",
            file_system->MkCacheParm("CVMFS_CACHE_TYPE", "default"));
  delete file_system;
}


TEST_F(T_MountPoint, CheckInstanceName) {
  FileSystem *fs = FileSystem::Create(fs_info_);
  ASSERT_TRUE(fs != NULL);

  EXPECT_TRUE(fs->CheckInstanceName("ceph"));
  EXPECT_TRUE(fs->CheckInstanceName("cephCache_01"));
  EXPECT_FALSE(fs->CheckInstanceName("ceph cache 01"));
  EXPECT_FALSE(fs->CheckInstanceName("aNameThatIsLongerThanItShouldBe"));
  delete fs;
}


TEST_F(T_MountPoint, CheckPosixCacheSettings) {
  FileSystem *fs = FileSystem::Create(fs_info_);
  ASSERT_TRUE(fs != NULL);

  FileSystem::PosixCacheSettings settings;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.is_alien = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.is_shared = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  settings.is_shared = false;
  settings.is_managed = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  settings.is_alien = false;
  settings.is_shared = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  fs->type_ = FileSystem::kFsLibrary;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  fs->type_ = FileSystem::kFsFuse;
  settings.cache_base_defined = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.cache_dir_defined = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  delete fs;
}


TEST_F(T_MountPoint, TriageCacheMgr) {
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("default", fs->cache_mgr_instance());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "string with spaces");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "foo");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "default");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("default", fs->cache_mgr_instance());
  }
}


TEST_F(T_MountPoint, RamCacheMgr) {
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ram_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ram_SIZE", "75");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("ram", fs->cache_mgr_instance());
    EXPECT_EQ(kRamCacheManager, fs->cache_mgr()->id());
  }
  options_mgr_.SetValue("CVMFS_CACHE_ram_MALLOC", "unknown");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_ram_MALLOC", "libc");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }
}


TEST_F(T_MountPoint, TieredCacheMgr) {
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_TYPE", "tiered");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_tiered_UPPER", "ram_upper");
  options_mgr_.SetValue("CVMFS_CACHE_ram_upper_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ram_upper_SIZE", "75");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "posix_lower");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_BASE", tmp_path_);
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_QUOTA_LIMIT", "0");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
    EXPECT_EQ(kRamCacheManager, reinterpret_cast<TieredCacheManager *>(
      fs->cache_mgr())->upper_->id());
    EXPECT_EQ(kPosixCacheManager, reinterpret_cast<TieredCacheManager *>(
      fs->cache_mgr())->lower_->id());
    EXPECT_FALSE(fs->cache_mgr()->LoadBreadcrumb(fs_info_.name).IsValid());
  }

  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "ram_lower");
  options_mgr_.SetValue("CVMFS_CACHE_ram_lower_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ram_lower_SIZE", "75");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
  }

  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "tiered");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
}


TEST_F(T_MountPoint, TieredComplex) {
  options_mgr_.SetValue("CVMFS_WORKSPACE", ".");
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_UPPER", "tiered_upper");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_UPPER", "uu_ram");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_LOWER", "ul_ram");
  options_mgr_.SetValue("CVMFS_CACHE_uu_ram_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_uu_ram_SIZE", "75");
  options_mgr_.SetValue("CVMFS_CACHE_ul_ram_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ul_ram_SIZE", "75");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "tiered_lower");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_UPPER", "lu_posix");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_LOWER", "ll_posix");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_BASE", tmp_path_ + "/lu");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_QUOTA_LIMIT", "0");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_BASE", tmp_path_ + "/ll");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_QUOTA_LIMIT", "0");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status()) << fs->boot_error();
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
    TieredCacheManager *upper = reinterpret_cast<TieredCacheManager *>(
      reinterpret_cast<TieredCacheManager *>(fs->cache_mgr())->upper_);
    TieredCacheManager *lower = reinterpret_cast<TieredCacheManager *>(
      reinterpret_cast<TieredCacheManager *>(fs->cache_mgr())->lower_);
    EXPECT_EQ(kRamCacheManager, upper->upper_->id());
    EXPECT_EQ(kRamCacheManager, upper->lower_->id());
    EXPECT_EQ(kPosixCacheManager, lower->upper_->id());
    EXPECT_EQ(kPosixCacheManager, lower->lower_->id());
    EXPECT_FALSE(fs->cache_mgr()->LoadBreadcrumb(fs_info_.name).IsValid());
  }
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
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }
  options_mgr_.UnsetValue("CVMFS_NFS_SOURCE");

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
    EXPECT_EQ(tmp_path_ + "/alien",
              reinterpret_cast<PosixCacheManager *>(
                fs->cache_mgr())->cache_path());
  }

  fs_info_.type = FileSystem::kFsFuse;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/alien",
              reinterpret_cast<PosixCacheManager *>(
                fs->cache_mgr())->cache_path());
  }

  RemoveTree(tmp_path_ + "/unit-test");
  options_mgr_.UnsetValue("CVMFS_ALIEN_CACHE");
  options_mgr_.SetValue("CVMFS_NFS_SOURCE", "yes");
  options_mgr_.SetValue("CVMFS_NFS_SHARED", tmp_path_ + "/nfs");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_TRUE(fs->IsNfsSource());
    EXPECT_TRUE(fs->IsHaNfsSource());
    EXPECT_EQ(".", reinterpret_cast<PosixCacheManager *>(
      fs->cache_mgr())->cache_path());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/nfs", fs->nfs_maps_dir_);
  }

  options_mgr_.SetValue("CVMFS_CACHE_DIR", tmp_path_ + "/cachedir_direct");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.UnsetValue("CVMFS_CACHE_BASE");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
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
  int pipe_sync[2];
  MakePipe(pipe_sync);

  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      SafeRead(pipe_sync[0], &pid, sizeof(pid));
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
  SafeWrite(pipe_sync[1], &pid, sizeof(pid));
  int stat_loc;
  int retval = waitpid(pid, &stat_loc, 0);
  EXPECT_NE(retval, -1);
  EXPECT_TRUE(WIFEXITED(stat_loc));
  EXPECT_TRUE(((fs01->boot_status() == 0) && (WEXITSTATUS(stat_loc) == 1)) ||
              ((fs01->boot_status() == loader::kFailLockWorkspace) &&
                  (WEXITSTATUS(stat_loc) == 0)));
  ClosePipe(pipe_sync);
}


TEST_F(T_MountPoint, UuidCache) {
  string cached_uuid;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    cached_uuid = fs->uuid_cache()->uuid();
  }
  ASSERT_EQ(fchdir(fd_cwd_), 0);

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

TEST_F(T_MountPoint, MountLatest) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  ASSERT_TRUE(HasSuffix(repo_path_, "repo", false));
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  string root_hash;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_EQ(root_hash, mp->catalog_mgr()->GetRootHash().ToString());
    EXPECT_TRUE(fs->cache_mgr()->LoadBreadcrumb("keys.cern.ch").IsValid());
  }

  // Again to check proper cleanup
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }
}


TEST_F(T_MountPoint, MountMulti) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  UniquePtr<MountPoint> mp01(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
  EXPECT_EQ(loader::kFailOk, mp01->boot_status());

  UniquePtr<MountPoint> mp02(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
  EXPECT_EQ(loader::kFailOk, mp02->boot_status());
}


TEST_F(T_MountPoint, MountErrors) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    string root_hash;
    EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash));
    EXPECT_EQ(root_hash, mp->catalog_mgr()->GetRootHash().ToString());
  }

  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("wrong.name", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_UID_MAP", "/no/such/file");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOptions, mp->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_HTTP_PROXY");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailWpad, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_PUBLIC_KEY", "/no/such/key");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailSignature, mp->boot_status());
  }
}


TEST_F(T_MountPoint, Blacklist) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  EXPECT_TRUE(MkdirDeep(repo_path_ + "/config.test/etc/cvmfs", 0700, true));
  options_mgr_.SetValue("CVMFS_MOUNT_DIR", repo_path_);
  options_mgr_.SetValue("CVMFS_CONFIG_REPOSITORY", "config.test");
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_TRUE(mp->ReloadBlacklists());
  }
  RemoveTree("cvmfs_ut_cache");

  string bad_revision = "<keys.cern.ch 1000";
  EXPECT_TRUE(SafeWriteToFile(bad_revision,
                              repo_path_ + "/config.test/etc/cvmfs/blacklist",
                              0600));
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailRevisionBlacklisted, mp->boot_status());
  }

  string bad_fingerprint =
    "00:7C:FA:EE:1A:2B:98:74:5D:14:A6:25:4E:C4:40:BC:BD:44:47:A3\n";
  EXPECT_TRUE(SafeWriteToFile(bad_fingerprint,
                              repo_path_ + "/config.test/etc/cvmfs/blacklist",
                              0600));
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_CONFIG_REPOSITORY");
  options_mgr_.SetValue("CVMFS_BLACKLIST", "/no/such/file");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_TRUE(mp->ReloadBlacklists());
  }
  RemoveTree("cvmfs_ut_cache");

  options_mgr_.SetValue("CVMFS_BLACKLIST",
                        repo_path_ + "/config.test/etc/cvmfs/blacklist");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }
}


TEST_F(T_MountPoint, History) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "1984-03-04T00:00:00Z");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailHistory, mp->boot_status());
  }

  if (sizeof(time_t) > 32) {
    options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "2424-01-01T00:00:00Z");
  } else {
    options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "2038-01-01T00:00:00Z");
  }
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_REPOSITORY_TAG", "no-such-tag");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailHistory, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_REPOSITORY_TAG", "snapshot");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }
}


TEST_F(T_MountPoint, MaxServers) {
  CreateMiniRepository(&options_mgr_, &repo_path_);
  string server_url;
  ASSERT_TRUE(options_mgr_.GetValue("CVMFS_SERVER_URL", &server_url));
  server_url += ";" + server_url;
  options_mgr_.SetValue("CVMFS_SERVER_URL", server_url);

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  std::vector<std::string> host_chain;

  options_mgr_.SetValue("CVMFS_MAX_SERVERS", "10");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    ASSERT_EQ(loader::kFailOk, mp->boot_status());
    mp->download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(2U, host_chain.size());
    mp->external_download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(1U, host_chain.size());
  }

  options_mgr_.SetValue("CVMFS_EXTERNAL_MAX_SERVERS", "10");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    ASSERT_EQ(loader::kFailOk, mp->boot_status());
    mp->external_download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    // Host chain has been set to one empty string in SetupExternalDownloadMgr
    EXPECT_EQ(1U, host_chain.size());
  }

  options_mgr_.SetValue("CVMFS_EXTERNAL_URL", server_url);
  options_mgr_.SetValue("CVMFS_MAX_SERVERS", "1");
  options_mgr_.SetValue("CVMFS_EXTERNAL_MAX_SERVERS", "1");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    ASSERT_EQ(loader::kFailOk, mp->boot_status());
    mp->download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(1U, host_chain.size());
    mp->external_download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(1U, host_chain.size());
  }

  options_mgr_.SetValue("CVMFS_MAX_SERVERS", "0");
  options_mgr_.SetValue("CVMFS_EXTERNAL_MAX_SERVERS", "0");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    ASSERT_EQ(loader::kFailOk, mp->boot_status());
    mp->download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(2U, host_chain.size());
    mp->external_download_mgr()->GetHostInfo(&host_chain, NULL, NULL);
    EXPECT_EQ(2U, host_chain.size());
  }
}
