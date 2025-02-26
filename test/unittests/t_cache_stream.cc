/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cache_posix.h>
#include <cache_stream.h>
#include <compression/compression.h>
#include <crypto/hash.h>
#include <network/download.h>
#include <statistics.h>
#include <util/pointer.h>
#include <util/posix.h>

class T_StreamingCacheManager : public ::testing::Test {
 protected:
  void StageFile(const std::string &content, shash::Any *hash) {
    void *zipped_buf;
    uint64_t zipped_size;
    zlib::CompressMem2Mem(content.data(),
                          static_cast<int64_t>(content.length()),
                          &zipped_buf, &zipped_size);
    std::string zipped_data(reinterpret_cast<char *>(zipped_buf), zipped_size);
    HashString(zipped_data, hash);
    EXPECT_TRUE(SafeWriteToFile(zipped_data, "data/" + hash->MakePath(), 0600));
  }

  virtual void SetUp() {
    statistics_ = new perf::Statistics();
    download_mgr_ = new download::DownloadManager(16,
                  perf::StatisticsTemplate("download", statistics_.weak_ref()));
    download_mgr_->SetHostChain("file://" + GetCurrentWorkingDirectory());
    backing_cache_ =
      PosixCacheManager::Create("cache", true /* alien_cache */);
    backing_cache_ref_ = backing_cache_.weak_ref();
    streaming_cache_ = new StreamingCacheManager(
      32, backing_cache_.Release(), download_mgr_.weak_ref(), NULL, 1000,
      statistics_.weak_ref());

    EXPECT_TRUE(MkdirDeep("data", 0700));
    EXPECT_TRUE(MakeCacheDirectories("data", 0700));
    hash_demo_.algorithm = shash::kShake128;
    demo_ = "Hello, World!";
    StageFile(demo_, &hash_demo_);
  }

  virtual void TearDown() {
    streaming_cache_.Destroy();
    download_mgr_.Destroy();
    statistics_.Destroy();
  }

  UniquePtr<perf::Statistics> statistics_;
  UniquePtr<download::DownloadManager> download_mgr_;
  UniquePtr<PosixCacheManager> backing_cache_;
  UniquePtr<StreamingCacheManager> streaming_cache_;

  CacheManager *backing_cache_ref_;
  std::string demo_;
  shash::Any hash_demo_;
};


TEST_F(T_StreamingCacheManager, Basics) {
  CacheManager::LabeledObject labeled_obj(hash_demo_);
  labeled_obj.label.size = demo_.length();

  int fd = streaming_cache_->Open(labeled_obj);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, streaming_cache_->counters().n_downloads->Get());
  EXPECT_EQ(static_cast<int64_t>(demo_.length()),
            streaming_cache_->GetSize(fd));
  EXPECT_EQ(1, streaming_cache_->counters().n_downloads->Get());
  EXPECT_EQ(1, streaming_cache_->counters().n_buffer_objects->Get());
  EXPECT_EQ(static_cast<int64_t>(demo_.length()),
            streaming_cache_->counters().sz_transferred_bytes->Get());
  char W = 0;
  EXPECT_EQ(1, streaming_cache_->Pread(fd, &W, 1, 7));
  EXPECT_EQ('W', W);
  EXPECT_EQ(1, streaming_cache_->counters().n_buffer_hits->Get());
  EXPECT_EQ(0, streaming_cache_->Close(fd));
  EXPECT_EQ(-ENOENT, backing_cache_ref_->Open(labeled_obj));
}


TEST_F(T_StreamingCacheManager, UnknownSize) {
  CacheManager::LabeledObject labeled_obj(hash_demo_);
  int fd = streaming_cache_->Open(labeled_obj);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(static_cast<int64_t>(demo_.length()),
            streaming_cache_->GetSize(fd));
  EXPECT_EQ(1, streaming_cache_->counters().n_downloads->Get());
  EXPECT_EQ(1, streaming_cache_->counters().n_buffer_obstacles->Get());
  EXPECT_EQ(static_cast<int64_t>(demo_.length()),
            streaming_cache_->counters().sz_transferred_bytes->Get());
  char W = 0;
  EXPECT_EQ(1, streaming_cache_->Pread(fd, &W, 1, 7));
  EXPECT_EQ('W', W);
  EXPECT_EQ(0, streaming_cache_->Close(fd));
  EXPECT_EQ(-ENOENT, backing_cache_ref_->Open(labeled_obj));
}
