/**
 * This file is part of the CernVM File System.
 *
 * Unit tests for the fuse low-level callbacks in cvmfs.cc,
 * mocking libfuse in order to check responses directly.
 *
 * The callbacks are not very easy to mock, and a few tricks
 * are used: Since the cvmfs_* callbacks are file-static and
 * there is no header file, we include cvmfs.cc here with the
 * definition __TEST_CVMFS_MOCKFUSE instead of linking it.
 * That skips some definitions in cvmfs.cc that we can mock here
 * instead. libfuse itself is replaced at link time with mockfuse/fuse_stub.cc.
 */

// clang-format off
#include "mockfuse/fuse_stub.h"
#include "mockfuse/mock_mountpoint.h"
// clang-format on

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <errno.h>
#include <fcntl.h>

#include <map>
#include <string>

#include "../common/testutil.h"
#include "catalog_mgr.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "fuse_inode_gen.h"
#include "fuse_remount.h"
#include "glue_buffer.h"
#include "monitor.h"
#include "notification_client.h"
#include "options.h"
#include "shortstring.h"
#include "talk.h"
#include "util/pointer.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::SetArgPointee;


namespace {

/**
 * Inode and path resolution is driven from these tables rather than from a
 * catalog.  They belong to the fixture; the file-scope pointers only exist
 * because the resolution helpers below have to be free functions with the
 * signature cvmfs.cc expects.
 */
typedef std::map<fuse_ino_t, catalog::DirectoryEntry> DirentByInode;
typedef std::map<std::string, catalog::DirectoryEntry> DirentByPath;

DirentByInode *g_dirent_by_inode = NULL;
DirentByPath *g_dirent_by_path = NULL;

/**
 * Inodes must stay above catalog::AbstractCatalogManager<>::kInodeOffset (255)
 * so that the real, non-virtual MangleInode() passes them through unchanged.
 */
const fuse_ino_t kTestInode = 1000;
const char *const kTestPath = "/dir/file";

/**
 * Content hashes need a specified algorithm: a default-constructed shash::Any
 * has none, and hashing the chunk list of a chunked file would then abort.
 */
shash::Any TestHash(unsigned char fill) {
  shash::Any hash(shash::kSha1);
  memset(hash.digest, fill, shash::kDigestSizes[shash::kSha1]);
  return hash;
}

}  // anonymous namespace


namespace cvmfs {

MockFileSystem *file_system_ = NULL;
MockMountPoint *mount_point_ = NULL;
TalkManager *talk_mgr_ = NULL;
NotificationClient *notification_client_ = NULL;
Watchdog *watchdog_ = NULL;
FuseRemounter *fuse_remounter_ = NULL;
InodeGenerationInfo inode_generation_info_;


/**
 * Replaces the production helper.  Same signature and same meaning of the
 * return value.
 */
static bool GetDirentForInode(const fuse_ino_t ino,
                              catalog::DirectoryEntry *dirent) {
  const DirentByInode::const_iterator i = g_dirent_by_inode->find(ino);
  if (i == g_dirent_by_inode->end())
    return false;
  *dirent = i->second;
  return true;
}


/**
 * Replaces the production helper.  Returns the live inode, or 0 if the path is
 * unknown -- note this is a uint64_t and not a bool, callers test for "> 0".
 */
static uint64_t GetDirentForPath(const PathString &path,
                                 catalog::DirectoryEntry *dirent) {
  const DirentByPath::const_iterator i = g_dirent_by_path->find(
      path.ToString());
  if (i == g_dirent_by_path->end())
    return 0;
  *dirent = i->second;
  return i->second.inode();
}

}  // namespace cvmfs


static int Init(const loader::LoaderExports *loader_export) { return 0; }
static bool NeedsReadEnviron() { return false; }

#define __TEST_CVMFS_MOCKFUSE
#include "cvmfs.cc"  // NOLINT: the callbacks under test are file-static


class T_Cvmfs : public ::testing::Test {
 protected:
  T_Cvmfs() : options_mgr_(NULL) { }

  virtual void SetUp() {
    g_dirent_by_inode = new DirentByInode();
    g_dirent_by_path = new DirentByPath();

    options_mgr_ = new SimpleOptionsParser();
    // Keeps the in-memory caches small; nothing here touches a disk cache.
    options_mgr_->SetValue("CVMFS_MEMCACHE_SIZE", "1");

    cvmfs::file_system_ = MockFileSystem::Create(options_mgr_);
    ASSERT_TRUE(cvmfs::file_system_->IsValid());
    cvmfs::mount_point_ = MockMountPoint::Create("mockfuse.cern.ch",
                                                 cvmfs::file_system_,
                                                 options_mgr_);
    ASSERT_TRUE(cvmfs::mount_point_->IsValid());
    cvmfs::fuse_remounter_ = new FuseRemounter(
        cvmfs::mount_point_, &cvmfs::inode_generation_info_, NULL, false);

    cvmfs::max_open_files_ = 8192;
    cvmfs::check_fd_overflow_ = true;
  }

  virtual void TearDown() {
    // Runs even when a test fails an assertion 
    // FileSystem allows only one live instance per process,
    // the next test would otherwise abort in its ctor
    delete cvmfs::fuse_remounter_;
    cvmfs::fuse_remounter_ = NULL;
    delete cvmfs::mount_point_;
    cvmfs::mount_point_ = NULL;
    delete cvmfs::file_system_;
    cvmfs::file_system_ = NULL;
    delete options_mgr_;
    options_mgr_ = NULL;

    delete g_dirent_by_inode;
    g_dirent_by_inode = NULL;
    delete g_dirent_by_path;
    g_dirent_by_path = NULL;
  }

  /**
   * Makes ino resolvable by GetPathForInode() and by the dirent tables.
   */
  catalog::DirectoryEntry PublishRegularFile(fuse_ino_t ino,
                                             const std::string &path) {
    catalog::DirectoryEntry dirent =
        catalog::DirectoryEntryTestFactory::RegularFile(GetFileName(path), 4096,
                                                        TestHash(0x01));
    dirent.set_inode(ino);
    RegisterDirent(ino, path, dirent);
    return dirent;
  }

  catalog::DirectoryEntry PublishChunkedFile(fuse_ino_t ino,
                                             const std::string &path) {
    catalog::DirectoryEntry dirent =
        catalog::DirectoryEntryTestFactory::ChunkedFile(TestHash(0x02));
    dirent.set_inode(ino);
    RegisterDirent(ino, path, dirent);
    return dirent;
  }

  /**
   * The page cache tracker keeps an entry after the last close so that the
   * kernel page cache can be reused; "still open" is the observable that
   * distinguishes a live reference from a released one.
   */
  bool IsTrackedOpen(fuse_ino_t ino) {
    shash::Any hash;
    struct stat info;
    return cvmfs::mount_point_->page_cache_tracker()->GetInfoIfOpen(ino, &hash,
                                                                    &info);
  }

  void RegisterDirent(fuse_ino_t ino,
                      const std::string &path,
                      const catalog::DirectoryEntry &dirent) {
    (*g_dirent_by_inode)[ino] = dirent;
    (*g_dirent_by_path)[path] = dirent;
    // GetPathForInode() is not stubbed out and consults the path cache first.
    cvmfs::mount_point_->path_cache()->Insert(ino, PathString(path));
  }

  /**
   * Mirrors what cvmfs_open() does on the way in, so that a release test can
   * start from a state the page cache tracker considers valid.
   */
  void TrackOpen(const catalog::DirectoryEntry &dirent) {
    cvmfs::mount_point_->page_cache_tracker()->Open(
        dirent.inode(), dirent.checksum(), dirent.GetStatStructure());
  }

  SimpleOptionsParser *options_mgr_;
};


TEST_F(T_Cvmfs, OpenRegularFile) {
  const catalog::DirectoryEntry dirent = PublishRegularFile(kTestInode,
                                                            kTestPath);
  EXPECT_CALL(*cvmfs::mount_point_->mock_fetcher(), Fetch(_, _))
      .WillOnce(Return(42));

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.flags = O_RDONLY;
  cvmfs::cvmfs_open(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyOpen, req.kind);
  EXPECT_EQ(42u, req.fi.fh);
  EXPECT_EQ(1, cvmfs::file_system_->no_open_files()->Get());
}


TEST_F(T_Cvmfs, OpenUnknownInodeRepliesEnoent) {
  // Resolvable as a path, but with no directory entry behind it.
  cvmfs::mount_point_->path_cache()->Insert(kTestInode, PathString(kTestPath));

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.flags = O_RDONLY;
  cvmfs::cvmfs_open(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyErr, req.kind);
  EXPECT_EQ(EIO, req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
}


TEST_F(T_Cvmfs, OpenExclusiveRepliesEexist) {
  PublishRegularFile(kTestInode, kTestPath);
  // O_EXCL is rejected before anything is fetched.
  EXPECT_CALL(*cvmfs::mount_point_->mock_fetcher(), Fetch(_, _)).Times(0);

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.flags = O_EXCL;
  cvmfs::cvmfs_open(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyErr, req.kind);
  EXPECT_EQ(EEXIST, req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
}


TEST_F(T_Cvmfs, OpenFailedFetchReportsErrorAndReleasesPageCacheEntry) {
  PublishRegularFile(kTestInode, kTestPath);
  EXPECT_CALL(*cvmfs::mount_point_->mock_fetcher(), Fetch(_, _))
      .WillOnce(Return(-EIO));

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.flags = O_RDONLY;
  cvmfs::cvmfs_open(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyErr, req.kind);
  EXPECT_EQ(EIO, req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
  // The tracker entry taken before the fetch has to be given back, otherwise
  // the inode would stay open forever and the next release would underflow.
  EXPECT_FALSE(IsTrackedOpen(kTestInode));
}


TEST_F(T_Cvmfs, OpenChunkedFile) {
  const catalog::DirectoryEntry dirent = PublishChunkedFile(kTestInode,
                                                            kTestPath);
  NiceMock<MockCatalogManager> *catalog_mgr =
      cvmfs::mount_point_->mock_catalog_mgr();
  EXPECT_CALL(*catalog_mgr, LookupPath(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(dirent), Return(true)));
  EXPECT_CALL(*catalog_mgr, ListFileChunks(_, _, _))
      .WillOnce(DoAll(
          testing::Invoke([](const PathString & /*path*/,
                             const shash::Algorithms /*algorithm*/,
                             FileChunkList *chunks) {
            chunks->PushBack(FileChunk(TestHash(0x03), 0, 4096));
          }),
          Return(true)));
  // A chunked open defers all downloading to read().
  EXPECT_CALL(*cvmfs::mount_point_->mock_fetcher(), Fetch(_, _)).Times(0);

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.flags = O_RDONLY;
  cvmfs::cvmfs_open(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyOpen, req.kind);
  // Chunked files are handed out as a negative handle.
  EXPECT_LT(static_cast<int64_t>(req.fi.fh), 0);
  EXPECT_EQ(1, cvmfs::file_system_->no_open_files()->Get());

  // The chunk list handed to the chunk tables is owned by whoever releases the
  // handle, so an unmatched open would leak it.  ~ChunkTables does not free it
  // either; that is cvmfs_release()'s job.
  fuse_req release_req;
  cvmfs::cvmfs_release(&release_req, kTestInode, &req.fi);
  ASSERT_EQ(fuse_req::kReplyErr, release_req.kind);
  ASSERT_EQ(0, release_req.err);
}


TEST_F(T_Cvmfs, ReleaseRegularFile) {
  const catalog::DirectoryEntry dirent = PublishRegularFile(kTestInode,
                                                            kTestPath);
  TrackOpen(dirent);
  perf::Xadd(cvmfs::file_system_->no_open_files(), 1);
  EXPECT_CALL(*cvmfs::file_system_->mock_cache_mgr(), Close(42))
      .WillOnce(Return(0));

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.fh = 42;
  cvmfs::cvmfs_release(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyErr, req.kind);
  EXPECT_EQ(0, req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
  EXPECT_FALSE(IsTrackedOpen(kTestInode));
}


TEST_F(T_Cvmfs, ReleaseChunkedFile) {
  const catalog::DirectoryEntry dirent = PublishChunkedFile(kTestInode,
                                                            kTestPath);
  TrackOpen(dirent);
  perf::Xadd(cvmfs::file_system_->no_open_files(), 1);

  // Set up the state that a chunked cvmfs_open() would have left behind: a
  // negative file handle pointing into the chunk tables.
  const uint64_t chunk_handle = 1;
  ChunkTables *chunk_tables = cvmfs::mount_point_->chunk_tables();
  ChunkFd chunk_fd;
  chunk_fd.fd = 200;
  chunk_fd.chunk_idx = 0;
  chunk_tables->handle2uniqino.Insert(chunk_handle, kTestInode);
  chunk_tables->handle2fd.Insert(chunk_handle, chunk_fd);
  chunk_tables->inode2references.Insert(kTestInode, 1);
  chunk_tables->inode2chunks.Insert(
      kTestInode,
      FileChunkReflist(new FileChunkList(), PathString(kTestPath),
                       zlib::kZlibDefault, false));

  EXPECT_CALL(*cvmfs::file_system_->mock_cache_mgr(), Close(200))
      .WillOnce(Return(0));

  fuse_req req;
  struct fuse_file_info fi = {};
  fi.fh = static_cast<uint64_t>(-static_cast<int64_t>(chunk_handle));
  cvmfs::cvmfs_release(&req, kTestInode, &fi);

  EXPECT_EQ(1, req.n_replies);
  EXPECT_EQ(fuse_req::kReplyErr, req.kind);
  EXPECT_EQ(0, req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
  // The last reference is gone, so the chunk bookkeeping must be empty again.
  EXPECT_FALSE(chunk_tables->handle2fd.Contains(chunk_handle));
  EXPECT_FALSE(chunk_tables->handle2uniqino.Contains(chunk_handle));
  EXPECT_FALSE(chunk_tables->inode2references.Contains(kTestInode));
  EXPECT_FALSE(chunk_tables->inode2chunks.Contains(kTestInode));
}


/**
 * An open followed by a release must leave no trace, which is what makes the
 * two callbacks safe to repeat.  This is the regression the suite exists for.
 */
TEST_F(T_Cvmfs, OpenReleaseRoundTripLeavesNoState) {
  PublishRegularFile(kTestInode, kTestPath);
  EXPECT_CALL(*cvmfs::mount_point_->mock_fetcher(), Fetch(_, _))
      .WillOnce(Return(42));
  EXPECT_CALL(*cvmfs::file_system_->mock_cache_mgr(), Close(42))
      .WillOnce(Return(0));

  fuse_req open_req;
  struct fuse_file_info fi = {};
  fi.flags = O_RDONLY;
  cvmfs::cvmfs_open(&open_req, kTestInode, &fi);
  ASSERT_EQ(fuse_req::kReplyOpen, open_req.kind);
  ASSERT_TRUE(IsTrackedOpen(kTestInode));

  fuse_req release_req;
  cvmfs::cvmfs_release(&release_req, kTestInode, &open_req.fi);

  EXPECT_EQ(1, release_req.n_replies);
  EXPECT_EQ(0, release_req.err);
  EXPECT_EQ(0, cvmfs::file_system_->no_open_files()->Get());
  EXPECT_FALSE(IsTrackedOpen(kTestInode));
}
