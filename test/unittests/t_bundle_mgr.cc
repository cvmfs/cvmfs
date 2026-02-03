/**
 * This file is part of the CernVM File System.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <type_traits>
#include <vector>

#include "bundle_mgr.h"
#include "catalog_mgr_client.h"
#include "fetch.h"
#include "file_bundle.h"
#include "glue_buffer.h"
#include "lru_md.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "testutil.h"
#include "util/pointer.h"
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

class MockFetcher : public cvmfs::Fetcher {
 public:
  MockFetcher(CacheManager *cache_mgr,
              download::DownloadManager *download_mgr,
              BackoffThrottle *backoff_throttle,
              perf::StatisticsTemplate statistics)
      : Fetcher(cache_mgr, download_mgr, backoff_throttle, statistics)
      , statistics_(statistics.statistics()) { };
  int Fetch(const CacheManager::LabeledObject &object,
            const std::string &alt_url = "") override {
    return ++counter_;
  }
  virtual ~MockFetcher() { delete statistics_; }
  void Reset() { counter_ = 0; }

  size_t counter_ = 0;
  perf::Statistics *statistics_;
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
  MOCK_METHOD(UniquePtr<CacheManager::LabeledObject>, GetNext, (), (const));
  void Reset() { counter_ = 0; }
  size_t counter_ = 0;
};


class T_BundleMgr : public ::testing::Test {
 protected:
  virtual void SetUp() {
    repo_path_ = "repo";
    uuid_dummy_ = cvmfs::Uuid::Create("");
    used_fds_ = GetNoUsedFds();
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
    mock_fetcher_ = new testing::NiceMock<MockFetcher>(
        nullptr,
        nullptr,
        nullptr,
        perf::StatisticsTemplate("fetch", new perf::Statistics()));
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
    ON_CALL(*bfm_, GetNext).WillByDefault([this]() {
      if (bfm_->counter_ < bfm_->Size()) {
        bfm_->counter_ += 1;
        shash::Any hash;
        CacheManager::Label label;
        label.path = std::string{};
        label.size = sizeof(shash::Any);
        label.zip_algorithm = zlib::kZlibDefault;
        return UniquePtr<CacheManager::LabeledObject>(
            new CacheManager::LabeledObject(hash, label));

      } else {
        return UniquePtr<CacheManager::LabeledObject>{nullptr};
      }
    });
    if (bundle_mgr_->fetcher_ != nullptr) {
      delete bundle_mgr_->fetcher_;
    }

    bundle_mgr_->fetcher_ = mock_fetcher_;
    // replace the real bfm_ with the mock
    delete bundle_mgr_->bfm_;
    bundle_mgr_->bfm_ = bfm_;
    EXPECT_TRUE(bundle_mgr_);
    EXPECT_EQ(bundle_mgr_->bfm_, bfm_);
    MakePipe(common_pipe_);
  }

  virtual void TearDown() {
    delete uuid_dummy_;
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    if (repo_path_ != "")
      RemoveTree(repo_path_);
    //    EXPECT_EQ(used_fds_, GetNoUsedFds()) << ShowOpenFiles();
    delete file_system_;
    delete mount_point_;
    delete mock_fetcher_;
    delete bundle_mgr_;
    ClosePipe(common_pipe_);
  }

  template<typename CT,
           typename = std::enable_if_t<std::is_trivially_copyable_v<CT> > >
  void test_blocking_exchange(const CT &obj) {
    using T = std::remove_cv_t<CT>;
    bundle_mgr_->BlockingSend(wfd_, obj);
    T reply = bundle_mgr_->BlockingReceive<T>(rfd_);
    EXPECT_EQ(obj, reply);
  }
  void test_blocking_exchange(const std::string&obj) {
    bundle_mgr_->BlockingSend(wfd_, obj);
    std::string reply = bundle_mgr_->BlockingReceive(rfd_);
    EXPECT_EQ(obj, reply);
  }

 protected:
  MountPoint *mount_point_;
  FileSystem *file_system_;
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  string tmp_path_;
  string repo_path_;
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

  // Mocks
  testing::NiceMock<MockBundleFileMgr> *bfm_;
  testing::NiceMock<MockPathCache> *mock_path_cache_;
  testing::NiceMock<MockInodeCache> *mock_inode_cache_;
  testing::NiceMock<MockCatalogManager> *mock_catalog_mgr_;
  testing::NiceMock<MockInodeTracker> *mock_inode_tracker_;
  testing::NiceMock<MockFetcher> *mock_fetcher_;

  int common_pipe_[2];
  int &rfd_ = common_pipe_[0];
  int &wfd_ = common_pipe_[1];
};

TEST_F(T_BundleMgr, ExchangeCT) {
  int integer = 42;
  shash::Any hash;
  uint64_t size = 42;
  zlib::Algorithms algo{zlib::Algorithms::kNoCompression};
  off_t offset = 42;
  hash.Randomize(integer);
  std::string string="Test_String";

  test_blocking_exchange(integer);
  test_blocking_exchange(hash);
  test_blocking_exchange(size);
  test_blocking_exchange(algo);
  test_blocking_exchange(offset);
  test_blocking_exchange(string);
}

TEST_F(T_BundleMgr, ExchangeLabeledObjects) { 
  shash::Any hash;
  hash.Randomize(42);
  CacheManager::Label label{};
  UniquePtr<CacheManager::LabeledObject> object {new CacheManager::LabeledObject{hash,label}};

  bundle_mgr_->SendLabeledObject(wfd_, object);
  UniquePtr<CacheManager::LabeledObject> replied_obj = bundle_mgr_->ReceiveLabeledObject(rfd_);
  EXPECT_TRUE(replied_obj.IsValid());

  EXPECT_EQ(object->id,replied_obj->id);
  EXPECT_EQ(object->label.flags,replied_obj->label.flags);
  EXPECT_EQ(object->label.size,replied_obj->label.size);
  EXPECT_EQ(object->label.zip_algorithm,replied_obj->label.zip_algorithm);
  EXPECT_EQ(object->label.range_offset,replied_obj->label.range_offset);
  EXPECT_EQ(object->label.path,replied_obj->label.path);
}

TEST_F(T_BundleMgr, Fetch) {
  bfm_->Reset();
  bundle_mgr_->Fetch();
  EXPECT_EQ(bfm_->counter_, bfm_->Size());
  EXPECT_EQ(mock_fetcher_->counter_, bfm_->Size());
}

