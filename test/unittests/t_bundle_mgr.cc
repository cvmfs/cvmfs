/**
 * This file is part of the CernVM File System.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <vector>

#include "bundle_mgr.h"
#include "catalog_mgr_client.h"
#include "fetch.h"
#include "file_bundle.h"
#include "json_document.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "util/posix.h"

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
    ++counter_;
    // Return an invalid file descriptor on purpose. The mock does not place
    // anything in the cache, so handing back a small fabricated fd (e.g. 1)
    // would make the production code perform a real Pread()/Close() on an
    // unrelated process file descriptor such as stdout.
    return -1;
  }
  virtual ~MockFetcher() { delete statistics_; }
  void Reset() { counter_ = 0; }

  // Shared across all fetcher instances and incremented from several worker
  // threads concurrently, hence atomic.
  inline static std::atomic<size_t> counter_{0};
  perf::Statistics *statistics_;
};

class T_BundleMgr : public ::testing::Test {
 protected:
  virtual void SetUp() {
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
    mock_catalog_mgr_ = new testing::NiceMock<MockCatalogManager>(mount_point_);
    mock_fetcher_ = new testing::NiceMock<MockFetcher>(
        nullptr,
        nullptr,
        nullptr,
        perf::StatisticsTemplate("fetch", new perf::Statistics()));
    mock_external_fetcher_ = new testing::NiceMock<MockFetcher>(
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
    // Plug mocks on mount_point_
    mount_point_->catalog_mgr_ = mock_catalog_mgr_;
    mount_point_->fetcher_ = mock_fetcher_;
    mount_point_->external_fetcher_ = mock_external_fetcher_;

    bundle_mgr_ = new BundleMgr(mount_point_, trigger_path_);

    bfm_ = new BundleFileMgr(JsonDocument::Create(CreateJsonTxt()));

    delete bundle_mgr_->bfm_;
    bundle_mgr_->bfm_ = bfm_;
    // Under the mocked fetcher the constructor cannot load the real bundle
    // file, so it leaves the manager invalid and without a worker pool. Put
    // it into a valid state around the injected bfm_ and start the pool so
    // that Fetch() actually dispatches work.
    bundle_mgr_->is_valid_ = true;
    bundle_mgr_->SpawnFetcherPool();
    // Count only the dependency fetches triggered by Fetch(), not the single
    // probe the constructor performed while trying to load the bundle file.
    MockFetcher::counter_ = 0;
    EXPECT_TRUE(bundle_mgr_);
    EXPECT_EQ(bundle_mgr_->bfm_, bfm_);
    MakePipe(common_pipe_);
  }

  virtual void TearDown() {
    // Destroy the BundleMgr first: its destructor joins the worker threads,
    // which may still dereference mount_point_/file_system_ while draining
    // the queue.
    delete bundle_mgr_;
    delete mount_point_;
    delete file_system_;
    ClosePipe(common_pipe_);
    EXPECT_EQ(0, chdir("../.."));
    if (not tmp_path_.empty()) {
      EXPECT_TRUE(RemoveTree(tmp_path_));
    }
  }

  template<typename CT,
           typename = std::enable_if_t<std::is_trivially_copyable_v<CT> > >
  void test_blocking_exchange(const CT &obj) {
    using T = std::remove_cv_t<CT>;
    bundle_mgr_->BlockingSend(wfd_, obj);
    T reply = bundle_mgr_->BlockingReceive<T>(rfd_);
    EXPECT_EQ(obj, reply);
  }
  void test_blocking_exchange(const std::string &obj) {
    bundle_mgr_->BlockingSend(wfd_, obj);
    std::string reply = bundle_mgr_->BlockingReceive(rfd_);
    EXPECT_EQ(obj, reply);
  }

 protected:
  MountPoint *mount_point_;
  FileSystem *file_system_;
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  std::string tmp_path_;

  PathString trigger_file_path_;
  BundleMgr *bundle_mgr_;

  catalog::DirectoryEntry trigger_dirent_{};
  PathString trigger_path_{};

  // Mocks
  BundleFileMgr *bfm_;
  testing::NiceMock<MockCatalogManager> *mock_catalog_mgr_;
  testing::NiceMock<MockFetcher> *mock_fetcher_;
  testing::NiceMock<MockFetcher> *mock_external_fetcher_;

  int common_pipe_[2];
  int &rfd_ = common_pipe_[0];
  int &wfd_ = common_pipe_[1];

  std::vector<std::string> dependencies_ = {"a_file_without_extension",
                                            "a_file_with.extension",
                                            "a/file/within/a/directory.foo"};

  std::string CreateJsonTxt() {
    std::ostringstream json;
    json << "{";
    json << "  \"name\": \"CVMFS_BUNDLE\",\n";
    json << "  \"version\": \"1.0.0\",\n";
    json << "  \"encoding\": \"UTF-8\",\n";
    json << "  \"dependencies\": [\n";
    for (size_t i = 0; i < dependencies_.size(); ++i) {
      json << "    \"" << dependencies_[i] << "\"";
      if (i < dependencies_.size() - 1) {
        json << ",\n";
      }
    }
    json << "]}";
    return json.str();
  }
};

TEST_F(T_BundleMgr, ExchangeCT) {
  int integer = 42;
  std::string string = "Test_String";

  test_blocking_exchange(integer);
  test_blocking_exchange(string);
}

TEST_F(T_BundleMgr, ExchangePathString) {
  PathString path("path/to/file.txt");

  bundle_mgr_->BlockingSend(wfd_, path);
  EXPECT_EQ(path, bundle_mgr_->ReceivePath(rfd_));
}

TEST_F(T_BundleMgr, Fetch) {
  bundle_mgr_->Fetch();
  // JoinFetcherPool() drains the work queue: every dependency is fetched
  // before the workers reach their terminate command, so the resulting count
  // is deterministic.
  bundle_mgr_->JoinFetcherPool();
  EXPECT_EQ(MockFetcher::counter_.load(), dependencies_.size());
}

