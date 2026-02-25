/**
 * This file is part of the CernVM File System.
 */


#define CVMFS_DUPLEX_FUSE_H_
#define __TEST_CVMFS_MOCKFUSE
#define CVMFS_USE_LIBFUSE 3
#define FUSE_USE_VERSION  31
#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include <fuse3/fuse_opt.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cache_posix.h"
#include "catalog_mgr.h"
#include "catalog_mgr_client.h"
#include "cvmfs.h"
#include "file_chunk.h"
#include "fuse_inode_gen.h"
#include "fuse_remount.h"
#include "glue_buffer.h"
#include "monitor.h"
#include "mountpoint.h"
#include "notification_client.h"
#include "options.h"
#include "quota_listener.h"
#include "quota_posix.h"
#include "talk.h"
#include "tracer.h"
#include "fetch.h"


using ::testing::_;
using ::testing::InSequence;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::StrictMock;

class MockMountPoint;
// Mock classes for testing cvmfs_release
class MockCatalogManager : public catalog::ClientCatalogManager {
 public:
  MockCatalogManager(MountPoint *mountpoint)
      : ClientCatalogManager(mountpoint) { };
  MOCK_METHOD(fuse_ino_t, MangleInode, (fuse_ino_t ino), (const));
};

class MockPageCacheTracker {
 public:
  MockPageCacheTracker() { }
  virtual ~MockPageCacheTracker() { }

  MOCK_METHOD(void, Close, (fuse_ino_t inode));
  MOCK_METHOD(bool, IsStale, (const catalog::DirectoryEntry &dirent), (const));
  MOCK_METHOD(glue::PageCacheTracker::EvictRaii, GetEvictRaii, ());
  MOCK_METHOD(bool, GetInfoIfOpen,
               (uint64_t inode, shash::Any *hash, struct stat *info));
  MOCK_METHOD(glue::PageCacheTracker::OpenDirectives, Open,
               (uint64_t inode,
                const shash::Any &hash,
                const struct stat &info));
  MOCK_METHOD(glue::PageCacheTracker::OpenDirectives, OpenDirect, ());
  MOCK_METHOD(void, Disable, ());
  MOCK_METHOD(void, Evict, (uint64_t inode));
};

class MockInodeTracker : public glue::InodeTracker {};

class MockTracer {
 public:
  MockTracer() { }
  virtual ~MockTracer() { }

  MOCK_METHOD(void, Activate, (const int buffer_size, const int flush_threshold,
                              const std::string &trace_file));
  MOCK_METHOD(void, Spawn, ());
  MOCK_METHOD(void, Flush, ());
  MOCK_METHOD(void, Trace, (const int event, const PathString &path,
                           const std::string &msg));
  MOCK_METHOD(bool, IsActive, (), (const));
};

class MockFetcher: public cvmfs::Fetcher {
 public:
  MockFetcher(CacheManager *cache_mgr,
          download::DownloadManager *download_mgr,
          BackoffThrottle *backoff_throttle,
          perf::StatisticsTemplate statistics):
    Fetcher(cache_mgr, download_mgr, backoff_throttle, statistics) { };

  int Fetch(const CacheManager::LabeledObject &object,
                 const std::string &alt_url="") {
    return MockFetch();
  }

  MOCK_METHOD(int, MockFetch, ());
};

class MockCacheManager : public PosixCacheManager {
 public:
  virtual int Close(int fd) override final { return 0; }
};

class MockMountPoint : public MountPoint {
 public:
  MockMountPoint(const std::string &fqrn,
                 FileSystem *file_system,
                 OptionsManager *options_mgr)
      : MountPoint(fqrn, file_system, options_mgr) { };
  ~MockMountPoint() { };
  NiceMock<MockPageCacheTracker> *page_cache_tracker() {
    return mock_page_cache_tracker_;
  }
  NiceMock<MockInodeTracker> *inode_tracker() { return mock_inode_tracker_; }
  NiceMock<MockCatalogManager> *catalog_mgr() { return mock_catalog_mgr_; }
  NiceMock<ChunkTables> *chunk_tables() { return mock_chunk_tables_; }
  NiceMock<lru::PathCache> *path_cache() { return mock_path_cache_; }
  NiceMock<lru::InodeCache> *inode_cache() { return mock_inode_cache_; }
  NiceMock<MockTracer> *tracer() { return mock_tracer_; }
  NiceMock<MockFetcher> *fetcher() { return mock_fetcher_; }
  NiceMock<MockFetcher> *external_fetcher() { return mock_external_fetcher_; }


  NiceMock<MockPageCacheTracker> *mock_page_cache_tracker_;
  NiceMock<MockInodeTracker> *mock_inode_tracker_;
  NiceMock<MockCatalogManager> *mock_catalog_mgr_;
  NiceMock<ChunkTables> *mock_chunk_tables_;
  NiceMock<lru::PathCache> *mock_path_cache_;
  NiceMock<lru::InodeCache> *mock_inode_cache_;
  NiceMock<MockTracer> *mock_tracer_;
  NiceMock<MockFetcher> *mock_fetcher_;
  NiceMock<MockFetcher> *mock_external_fetcher_;
};

class MockFileSystem : public FileSystem {
 public:
  void FixCustomVfs() { this->SetHasCustomVfs(false); }
  NiceMock<MockCacheManager> *cache_mgr() { return mock_cache_mgr_; }

  NiceMock<MockCacheManager> *mock_cache_mgr_;
};


static int g_fuseReplyErr = -999;
static catalog::DirectoryEntry *gDirentForInode = NULL;
static bool gDirentForInodeRetval = false;
static catalog::DirectoryEntry *gDirentForPath = NULL;
static bool gDirentForPathRetval = false;
namespace cvmfs {
NiceMock<MockFileSystem> *file_system_ = NULL;
NiceMock<MockMountPoint> *mount_point_ = NULL;
TalkManager *talk_mgr_ = NULL;
NotificationClient *notification_client_ = NULL;
Watchdog *watchdog_ = NULL;
FuseRemounter *fuse_remounter_ = NULL;
InodeGenerationInfo inode_generation_info_ = InodeGenerationInfo();
NiceMock<MockCacheManager> *mock_cache_mgr_ = NULL;

// Mock implementation of reply functions
int fuse_reply_err(fuse_req_t req, int err) {
  g_fuseReplyErr = err;  // TODO: set response in req instead of global for
                         // multithreaded tests
  printf("[FUSE REPLY] Error: %d (%s)\n", err, strerror(err));
  return 0;
}

int fuse_reply_open(fuse_req_t req, const struct fuse_file_info *fi) {
  g_fuseReplyErr = 0;
  printf("[FUSE REPLY] Open: fh=%ld, flags=%d\n", fi->fh, fi->flags);
  return 0;
}

// Mock implementation of fuse_req_ctx
struct fuse_ctx mock_fuse_ctx = {
    .uid = 1000, .gid = 1000, .pid = 12345, .umask = 022};

const struct fuse_ctx *fuse_req_ctx(fuse_req_t req) { return &mock_fuse_ctx; }


static void cvmfs_release_test(fuse_req_t req, fuse_ino_t ino,
                               struct fuse_file_info *fi) {
  const HighPrecisionTimer guard_timer(file_system_->hist_fs_release());
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  mount_point_->page_cache_tracker()->Close(ino);
  cvmfs::fuse_reply_err(req, 0);
}

static bool GetDirentForPath(const PathString &path,
                             catalog::DirectoryEntry *dirent) {
  *dirent = *gDirentForPath;
  return gDirentForPathRetval;
}


static bool GetDirentForInode(const fuse_ino_t,
                              catalog::DirectoryEntry *dirent) {
  *dirent = *gDirentForInode;
  return gDirentForInodeRetval;
}

}  // namespace cvmfs

static int Init(const loader::LoaderExports *loader_export) { return 0; }

#define fuse_reply_err cvmfs::fuse_reply_err
#define fuse_reply_open cvmfs::fuse_reply_open
#define fuse_req_ctx   cvmfs::fuse_req_ctx
#define Fetcher MockFetcher
#include "cvmfs.cc"


class T_Cvmfs : public ::testing::Test {
 public:
  void TestSetup() {
    cvmfs::max_open_files_ = 5678;
    gDirentForInode = new catalog::DirectoryEntry();
    gDirentForPath = new catalog::DirectoryEntry();
    // Set up FileSystem for use in FUSE callbacks
    // From t_mountpoint.cc, TODO(vvolkl): refactor
    repo_path_ = "repo";
    uuid_dummy_ = cvmfs::Uuid::Create("");
    used_fds_ = 1;  // GetNoUsedFds();
    fd_cwd_ = open(".", O_RDONLY);
    ASSERT_GE(fd_cwd_, 0);
    tmp_path_ = CreateTempDir("./cvmfs_ut_cache");
    options_mgr_ = new SimpleOptionsParser();
    options_mgr_->SetValue("CVMFS_CACHE_BASE", tmp_path_);
    options_mgr_->SetValue("CVMFS_SHARED_CACHE", "no");
    options_mgr_->SetValue("CVMFS_MAX_RETRIES", "0");
    fs_info_.name = "unit-test";
    fs_info_.options_mgr = options_mgr_;
    // Silence syslog error
    options_mgr_->SetValue("CVMFS_MOUNT_DIR", "/no/such/dir");
    cvmfs::file_system_ = static_cast<NiceMock<MockFileSystem> *>(
        FileSystem::Create(fs_info_));
    cvmfs::file_system_->FixCustomVfs();
    cvmfs::mount_point_ = static_cast<NiceMock<MockMountPoint> *>(
        MountPoint::Create("TestMountPoint", cvmfs::file_system_,
                           options_mgr_));
    mock_page_cache_tracker_ = new NiceMock<MockPageCacheTracker>();
    mock_inode_tracker_ = new NiceMock<MockInodeTracker>();
    cvmfs::fuse_remounter_ = new FuseRemounter(
        cvmfs::mount_point_, &cvmfs::inode_generation_info_, NULL, false);
    cvmfs::mount_point_
        ->mock_inode_tracker_ = static_cast<NiceMock<MockInodeTracker> *>(
        new glue::InodeTracker());
    mock_chunk_tables_ = new NiceMock<ChunkTables>();
    mock_catalog_mgr_ = new NiceMock<MockCatalogManager>(cvmfs::mount_point_);
    mock_cache_tmp_path_ = CreateTempDir("/tmp/cvmfs_mf_cache");
    mock_cache_mgr_ = static_cast<NiceMock<MockCacheManager> *>(
        PosixCacheManager::Create(mock_cache_tmp_path_, false,
                                  PosixCacheManager::kRenameNormal));
    cvmfs::file_system_->mock_cache_mgr_ = mock_cache_mgr_;
    cvmfs::mount_point_->mock_page_cache_tracker_ = mock_page_cache_tracker_;

    cvmfs::mount_point_->mock_catalog_mgr_ = mock_catalog_mgr_;
    cvmfs::mount_point_->mock_chunk_tables_ = mock_chunk_tables_;

    mock_path_cache_ = static_cast<NiceMock<lru::PathCache> *>(
        new lru::PathCache(64 * 1024, new perf::Statistics()));
    cvmfs::mount_point_->mock_path_cache_ = mock_path_cache_;
    mock_inode_cache_ = static_cast<NiceMock<lru::InodeCache> *>(
        new lru::InodeCache(64 * 1024, new perf::Statistics()));
    cvmfs::mount_point_->mock_inode_cache_ = mock_inode_cache_;
    mock_tracer_ = new NiceMock<MockTracer>();
    cvmfs::mount_point_->mock_tracer_ = mock_tracer_;
    mock_fetcher_ = new NiceMock<MockFetcher>(nullptr, nullptr, nullptr, perf::StatisticsTemplate("fetch", new perf::Statistics()));
    cvmfs::mount_point_->mock_fetcher_ = mock_fetcher_;
    mock_external_fetcher_ = new NiceMock<MockFetcher>(nullptr, nullptr, nullptr, perf::StatisticsTemplate("fetch-external", new perf::Statistics()));
    cvmfs::mount_point_->mock_external_fetcher_ = mock_external_fetcher_;
    ON_CALL(*mock_catalog_mgr_, MangleInode(_))
        .WillByDefault([](fuse_ino_t ino) { return ino; });
    ON_CALL(*mock_page_cache_tracker_, Close(_))
        .WillByDefault([](fuse_ino_t ino) { return 0; });
    ON_CALL(*mock_tracer_, Trace(_,_,_))
        .WillByDefault([](int event, const PathString &path,
                          const std::string &msg) { return; });

  }

  void TestTearDown() {
    delete mock_catalog_mgr_;
    delete mock_page_cache_tracker_;
    delete mock_inode_tracker_;
    delete mock_cache_mgr_;
    delete mock_path_cache_;
    delete mock_inode_cache_;
    delete mock_tracer_;
    delete mock_fetcher_;
    delete mock_external_fetcher_;

    cvmfs::file_system_->mock_cache_mgr_ = nullptr;
    cvmfs::mount_point_->mock_page_cache_tracker_ = nullptr;
    cvmfs::mount_point_->mock_inode_tracker_ = nullptr;
    cvmfs::mount_point_->mock_catalog_mgr_ = nullptr;
    cvmfs::mount_point_->mock_tracer_ = nullptr;
    cvmfs::mount_point_->mock_fetcher_ = nullptr;
    cvmfs::mount_point_->mock_external_fetcher_ = nullptr;
    delete cvmfs::file_system_;
    delete cvmfs::fuse_remounter_;

    delete gDirentForInode;
    delete gDirentForPath;
  }

 protected:
  virtual void SetUp() { }

  virtual void TearDown() {
    delete uuid_dummy_;
    int retval = fchdir(fd_cwd_);
    ASSERT_EQ(0, retval);
    close(fd_cwd_);
  }

 protected:
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser *options_mgr_;
  string tmp_path_;
  string mock_cache_tmp_path_;
  string repo_path_;
  int fd_cwd_;
  unsigned used_fds_;
  cvmfs::Uuid *uuid_dummy_;


  NiceMock<MockCatalogManager> *mock_catalog_mgr_;
  NiceMock<MockPageCacheTracker> *mock_page_cache_tracker_;
  NiceMock<MockInodeTracker> *mock_inode_tracker_;
  NiceMock<MockCacheManager> *mock_cache_mgr_;
  NiceMock<ChunkTables> *mock_chunk_tables_;
  NiceMock<lru::PathCache> *mock_path_cache_;
  NiceMock<lru::InodeCache> *mock_inode_cache_;
  NiceMock<MockTracer> *mock_tracer_;
  NiceMock<MockFetcher> *mock_fetcher_;
  NiceMock<MockFetcher> *mock_external_fetcher_;
};


TEST_F(T_Cvmfs, Dummy) {
  TestSetup();
  // are we able to see symbols from cvmfs.cc?
  ASSERT_NE(g_cvmfs_exports, nullptr);

  // Are we mocking the cache manager correctly?
  ASSERT_EQ(0, cvmfs::file_system_->cache_mgr()->Close(100));


  fuse_ino_t ino = 100;
  fuse_req_t mock_req = NULL;
  fuse_file_info *fi;

  EXPECT_CALL(*mock_page_cache_tracker_, Close(ino))
      .Times(1);  // Should be called once
  EXPECT_CALL(*mock_catalog_mgr_, MangleInode(ino))
      .Times(1);  // Should be called once

  cvmfs::cvmfs_release_test(mock_req, ino, fi);
  ASSERT_EQ(g_fuseReplyErr, 0);

  TestTearDown();
}


TEST_F(T_Cvmfs, cvmfs_release) {
  TestSetup();

  ASSERT_NE(g_cvmfs_exports, nullptr);


  fuse_ino_t ino = 100;
  fuse_req_t mock_req = NULL;
  fuse_file_info fi;


  // release non-chunked file
  fi.fh = 1;
  g_fuseReplyErr = -999;
  cvmfs::file_system_->no_open_files()->Set(1);

  EXPECT_CALL(*mock_page_cache_tracker_, Close(ino))
      .Times(1);  // Should be called once


  cvmfs::cvmfs_release(mock_req, ino, &fi);


  ASSERT_EQ(cvmfs::file_system_->no_open_files()->Get(), 0);
  ASSERT_EQ(g_fuseReplyErr, 0);


  ///////////// release of chunked file ////

  EXPECT_CALL(*mock_page_cache_tracker_, Close(ino))
      .Times(1);  // Should be called once
  g_fuseReplyErr = -999;


  cvmfs::file_system_->no_open_files()->Set(1);
  ChunkTables *chunk_tables = cvmfs::mount_point_->chunk_tables();

  fi.fh = -1;
  // chunk tables empty, expect failure
  EXPECT_DEATH_IF_SUPPORTED(cvmfs::cvmfs_release(mock_req, ino, &fi),
                            "Assertion");

  fi.fh = -1;
  const int64_t fd = static_cast<int64_t>(fi.fh);
  const uint64_t chunk_handle = (fd < 0) ? -fd : fd;
  ChunkFd chunk_fd;
  chunk_fd.fd = 200;
  chunk_fd.chunk_idx = 201;
  chunk_tables->handle2uniqino.Insert(chunk_handle, ino);
  chunk_tables->handle2fd.Insert(chunk_handle, chunk_fd);
  chunk_tables->inode2references.Insert(ino, 2);

  ChunkFd chunk_fd2;
  ASSERT_EQ(true, chunk_tables->handle2fd.Lookup(chunk_handle, &chunk_fd2));

  cvmfs::cvmfs_release(mock_req, ino, &fi);
  ASSERT_EQ(g_fuseReplyErr, 0);

  TestTearDown();
}


TEST_F(T_Cvmfs, cvmfs_open) {
  TestSetup();


  g_fuseReplyErr = -999;
  fuse_ino_t ino = 100;
  fuse_req_t mock_req = NULL;
  fuse_file_info fi;


  // release non-chunked file
  fi.fh = 1;
  cvmfs::file_system_->no_open_files()->Set(0);

  cvmfs::cvmfs_open(mock_req, ino, &fi);

  ASSERT_EQ(g_fuseReplyErr, 5);

  // expect to fail if O_EXCL is set
  fi.fh = 1;
  g_fuseReplyErr = -999;
  gDirentForInodeRetval = true;
  gDirentForPathRetval = true;
  fi.flags = O_EXCL;
  cvmfs::file_system_->no_open_files()->Set(0);


  cvmfs::cvmfs_open(mock_req, ino, &fi);

  ASSERT_EQ(g_fuseReplyErr, 17);


    // release non-chunked file
  fi.fh = 1;
  g_fuseReplyErr = -999;
  gDirentForInodeRetval = true;
  gDirentForPathRetval = true;
  fi.flags = O_RDONLY;
  cvmfs::file_system_->no_open_files()->Set(0);

    ON_CALL(*mock_fetcher_, MockFetch())
        .WillByDefault([]() { return 15; });

  cvmfs::cvmfs_open(mock_req, ino, &fi);

  ASSERT_EQ(g_fuseReplyErr, 0);

     // release non-chunked file
  fi.fh = 1;
  g_fuseReplyErr = -999;
  gDirentForInodeRetval = true;
  gDirentForPathRetval = true;
  fi.flags = O_RDONLY;
  cvmfs::file_system_->no_open_files()->Set(0);

    ON_CALL(*mock_fetcher_, MockFetch())
        .WillByDefault([]() { return 15; });

  cvmfs::cvmfs_open(mock_req, ino, &fi);

  ASSERT_EQ(g_fuseReplyErr, 0);

  TestTearDown();
}
