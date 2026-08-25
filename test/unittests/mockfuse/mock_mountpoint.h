/**
 * This file is part of the CernVM File System.
 *
 * Mocks for cvmfs.cc fuse callback dependencies.
 * Construct mocks as their final types.
 * Mock I/O collaborators; keep in-memory helpers real.
 */

#ifndef TEST_UNITTESTS_MOCKFUSE_MOCK_MOUNTPOINT_H_
#define TEST_UNITTESTS_MOCKFUSE_MOCK_MOUNTPOINT_H_

#include <gmock/gmock.h>

#include <string>

#include "backoff.h"
#include "cache.h"
#include "catalog_mgr_client.h"
#include "crypto/hash.h"
#include "fetch.h"
#include "file_chunk.h"
#include "loader.h"
#include "mountpoint.h"
#include "options.h"
#include "statistics.h"
#include "tracer.h"


/** Avoids closing real process fds. */
class MockCacheManager : public CacheManager {
 public:
  MOCK_METHOD(CacheManagerIds, id, (), (override));
  MOCK_METHOD(std::string, Describe, (), (override));
  MOCK_METHOD(bool, AcquireQuotaManager, (QuotaManager * quota_mgr),
              (override));
  MOCK_METHOD(int, Open, (const LabeledObject &object), (override));
  MOCK_METHOD(int64_t, GetSize, (int fd), (override));
  MOCK_METHOD(int, Close, (int fd), (override));
  MOCK_METHOD(int64_t, Pread,
              (int fd, void *buf, uint64_t size, uint64_t offset), (override));
  MOCK_METHOD(int, Dup, (int fd), (override));
  MOCK_METHOD(int, Readahead, (int fd), (override));
  MOCK_METHOD(uint32_t, SizeOfTxn, (), (override));
  MOCK_METHOD(int, StartTxn, (const shash::Any &id, uint64_t size, void *txn),
              (override));
  MOCK_METHOD(void, CtrlTxn, (const Label &label, const int flags, void *txn),
              (override));
  MOCK_METHOD(int64_t, Write, (const void *buf, uint64_t sz, void *txn),
              (override));
  MOCK_METHOD(int, Reset, (void *txn), (override));
  MOCK_METHOD(int, AbortTxn, (void *txn), (override));
  MOCK_METHOD(int, OpenFromTxn, (void *txn), (override));
  MOCK_METHOD(int, CommitTxn, (void *txn), (override));
  MOCK_METHOD(void, Spawn, (), (override));
};


class MockFetcher : public cvmfs::Fetcher {
 public:
  MockFetcher(CacheManager *cache_mgr,
              download::DownloadManager *download_mgr,
              BackoffThrottle *backoff_throttle,
              perf::StatisticsTemplate statistics)
      : Fetcher(cache_mgr, download_mgr, backoff_throttle, statistics) { }

  MOCK_METHOD(int, Fetch,
              (const CacheManager::LabeledObject &object,
               const std::string &alt_url),
              (override));
};


/** Mock lookups; keep inode helpers real. */
class MockCatalogManager : public catalog::ClientCatalogManager {
 public:
  explicit MockCatalogManager(MountPoint *mountpoint)
      : ClientCatalogManager(mountpoint) { }

  MOCK_METHOD(bool, LookupPath,
              (const PathString &path,
               const catalog::LookupOptions options,
               catalog::DirectoryEntry *entry),
              (override));
  MOCK_METHOD(bool, ListFileChunks,
              (const PathString &path,
               const shash::Algorithms interpret_hashes_as,
               FileChunkList *chunks),
              (override));
};


/** Constructed, not booted. */
class MockFileSystem : public FileSystem {
 public:
  static MockFileSystem *Create(OptionsManager *options_mgr) {
    FileSystemInfo info;
    info.name = "mockfuse";
    info.type = kFsFuse;
    info.options_mgr = options_mgr;

    MockFileSystem *file_system = new MockFileSystem(info);
    file_system->CreateStatistics();
    file_system->cache_mgr_ = new ::testing::NiceMock<MockCacheManager>();
    file_system->set_boot_status(loader::kFailOk);
    return file_system;
  }

  /** cache_mgr_ is a NiceMock<MockCacheManager>. */
  ::testing::NiceMock<MockCacheManager> *mock_cache_mgr() {
    return static_cast< ::testing::NiceMock<MockCacheManager> *>(cache_mgr_);
  }

 private:
  explicit MockFileSystem(const FileSystemInfo &info) : FileSystem(info) { }
};


/** Constructed, not booted; fill only dereferenced members. */
class MockMountPoint : public MountPoint {
 public:
  static MockMountPoint *Create(const std::string &fqrn,
                                MockFileSystem *file_system,
                                OptionsManager *options_mgr) {
    MockMountPoint *mount_point = new MockMountPoint(fqrn, file_system,
                                                     options_mgr);
    // Reuse in-memory setup helpers.
    mount_point->CreateStatistics();
    mount_point->CreateTables();
    mount_point->backoff_throttle_ = new BackoffThrottle();
    // Avoid CreateTracer(): it may write via CVMFS_TRACEFILE.
    mount_point->tracer_ = new Tracer();

    // Create fetchers before catalog_mgr_.
    mount_point->fetcher_ = new ::testing::NiceMock<MockFetcher>(
        file_system->cache_mgr(),
        static_cast<download::DownloadManager *>(NULL),
        mount_point->backoff_throttle_,
        perf::StatisticsTemplate("fetch", mount_point->statistics_));
    mount_point->external_fetcher_ = new ::testing::NiceMock<MockFetcher>(
        file_system->cache_mgr(),
        static_cast<download::DownloadManager *>(NULL),
        mount_point->backoff_throttle_,
        perf::StatisticsTemplate("fetch-external", mount_point->statistics_));
    mount_point->catalog_mgr_ = new ::testing::NiceMock<MockCatalogManager>(
        mount_point);

    mount_point->statfs_cache_ = new StatfsCache(0);

    // Leave magic_xattr_mgr_ NULL: these tests do not touch xattrs,
    // and MagicXattrManager currently leaks registered xattrs.

    mount_point->set_boot_status(loader::kFailOk);
    return mount_point;
  }

  ::testing::NiceMock<MockCatalogManager> *mock_catalog_mgr() {
    return static_cast< ::testing::NiceMock<MockCatalogManager> *>(
        catalog_mgr_);
  }
  ::testing::NiceMock<MockFetcher> *mock_fetcher() {
    return static_cast< ::testing::NiceMock<MockFetcher> *>(fetcher_);
  }
  ::testing::NiceMock<MockFetcher> *mock_external_fetcher() {
    return static_cast< ::testing::NiceMock<MockFetcher> *>(external_fetcher_);
  }

 private:
  MockMountPoint(const std::string &fqrn,
                 FileSystem *file_system,
                 OptionsManager *options_mgr)
      : MountPoint(fqrn, file_system, options_mgr) { }
};

#endif  // TEST_UNITTESTS_MOCKFUSE_MOCK_MOUNTPOINT_H_
