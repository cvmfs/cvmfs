/**
 * This file is part of the CernVM File System.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "bundle_mgr.h"
#include "catalog_mgr_client.h"
#include "file_bundle.h"
#include "glue_buffer.h"
#include "lru_md.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "testutil.h"
#include "util/uuid.h"

using namespace std;  // NOLINT
class MockCatalogManager : public catalog::ClientCatalogManager {
 public:
  MockCatalogManager(MountPoint *mountpoint)
      : ClientCatalogManager(mountpoint) { };
  virtual ~MockCatalogManager() = default;
  MOCK_METHOD(fuse_ino_t, MangleInode, (fuse_ino_t ino), (const));
  MOCK_METHOD(bool,
              LookupPath,
              (const PathString &path,
               const catalog::LookupOptions options,
               catalog::DirectoryEntry *dirent),
              (override));
};

class MockInodeTracker : public glue::InodeTracker {
 public:
  virtual ~MockInodeTracker() = default;
  MOCK_METHOD(bool,
              FindPath,
              (glue::InodeEx * inode_ex, PathString *path),
              (override));
};

class MockInodeCache : public lru::InodeCache {
 public:
  MockInodeCache(unsigned int cache_size, perf::Statistics *statistics)
      : lru::InodeCache(cache_size, statistics), statistics_(statistics) { }
  virtual ~MockInodeCache() { delete statistics_; }

 private:
  perf::Statistics *statistics_;
};

class MockPathCache : public lru::PathCache {
 public:
  MockPathCache(unsigned int cache_size, perf::Statistics *statistics)
      : lru::PathCache(cache_size, statistics), statistics_(statistics) { }
  virtual ~MockPathCache() { delete statistics_; }

 private:
  perf::Statistics *statistics_;
};

class MockBundleFileMgr : public BundleFileMgr {
 public:
  MockBundleFileMgr(const PathString &trigger_file_path)
      : BundleFileMgr(trigger_file_path) { };
  virtual ~MockBundleFileMgr() = default;
  MOCK_METHOD(size_t, Size, (), (const));
  MOCK_METHOD(CacheManager::LabeledObject *, GetNext, (), (const));
  void Reset(std::vector<CacheManager::LabeledObject *> &vec) {
    it = vec.begin();
  }
  std::vector<CacheManager::LabeledObject *>::iterator it;
};


class T_BundleMgr : public ::testing::Test {
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
    ASSERT_TRUE(file_system_ != NULL);
    EXPECT_TRUE(file_system_->IsValid());
    mount_point_ = MountPoint::Create("keys.cern.ch", file_system_);

    // MountPoint mocks allocation
    mock_path_cache_ = new testing::NiceMock<MockPathCache>(
        64 * 1024, new perf::Statistics());
    mock_inode_cache_ = new testing::NiceMock<MockInodeCache>(
        64 * 1024, new perf::Statistics());
    mock_catalog_mgr_ = new testing::NiceMock<MockCatalogManager>(mount_point_);
    mock_inode_tracker_ = new testing::NiceMock<MockInodeTracker>();

    // Determine mock behavior
    ON_CALL(*mock_catalog_mgr_, LookupPath(testing::_, testing::_, testing::_))
        .WillByDefault([this](const PathString &,
                              const catalog::LookupOptions,
                              catalog::DirectoryEntry *out) -> bool {
          *out = this->trigger_dirent_;
          return true;
        });
    ON_CALL(*mock_inode_tracker_, FindPath(testing::_, testing::_))
        .WillByDefault([this](glue::InodeEx *inode_ex,
                              PathString *path) -> bool {
          glue::InodeEx result(this->trigger_ino_, glue::InodeEx::kUnknownType);
          *inode_ex = result;
          *path = this->trigger_path_;
          return true;
        });
    // Plug mocks on mount_point_
    mount_point_->path_cache_ = mock_path_cache_;
    mount_point_->inode_cache_ = mock_inode_cache_;
    mount_point_->catalog_mgr_ = mock_catalog_mgr_;
    mount_point_->inode_tracker_ = mock_inode_tracker_;

    bundle_mgr_ = new BundleMgr(mount_point_, trigger_ino_);

    // Create a BundleFileMgr mock
    bfm_ = new testing::NiceMock<MockBundleFileMgr>(trigger_file_path_);
    ON_CALL(*bfm_, Size).WillByDefault(testing::Return(10));
    EXPECT_EQ(bfm_->Size(), 10);
    srand(time(NULL));
    shash::Any hash;
    for (size_t i = 0; i < bfm_->Size(); ++i) {
      CacheManager::Label label;
      label.path = std::string{};
      label.size = sizeof(hash);
      label.zip_algorithm = zlib::kZlibDefault;
      auto *obj = new CacheManager::LabeledObject(hash, label);
      entries_.push_back(obj);
    }
    EXPECT_EQ(entries_.size(), bfm_->Size());
    ON_CALL(*bfm_, GetNext)
        .WillByDefault([this]() -> CacheManager::LabeledObject * {
          auto &it = this->bfm_->it;
          auto end = this->entries_.end();
          auto res = it;
          if (it != end) {
            ++it;
            return *res;
          } else {
            return nullptr;
          }
        });
    // replace the real bfm_ with the mock
    delete bundle_mgr_->bfm_;
    bundle_mgr_->bfm_ = bfm_;
    EXPECT_TRUE(bundle_mgr_);
    EXPECT_EQ(bundle_mgr_->bfm_, bfm_);
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
    //    EXPECT_EQ(used_fds_, GetNoUsedFds()) << ShowOpenFiles();
    delete file_system_;
    delete mount_point_;
    delete bundle_mgr_;
    for (auto entry : entries_) {
      delete entry;
    }
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


  PathString trigger_file_path_;
  BundleMgr *bundle_mgr_;

  fuse_ino_t trigger_ino_{};
  catalog::DirectoryEntry trigger_dirent_{};
  PathString trigger_path_{};

  std::vector<CacheManager::LabeledObject *> entries_;

  // Mocks
  testing::NiceMock<MockBundleFileMgr> *bfm_;
  testing::NiceMock<MockPathCache> *mock_path_cache_;
  testing::NiceMock<MockInodeCache> *mock_inode_cache_;
  testing::NiceMock<MockCatalogManager> *mock_catalog_mgr_;
  testing::NiceMock<MockInodeTracker> *mock_inode_tracker_;
};

TEST_F(T_BundleMgr, Fetch) {
  bfm_->Reset(entries_);
  bundle_mgr_->Fetch();
}

