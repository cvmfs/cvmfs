/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "backoff.h"
#include "cache_posix.h"
#include "chunk_readahead.h"
#include "crypto/hash.h"
#include "file_chunk.h"
#include "statistics.h"
#include "util/mutex.h"
#include "util/posix.h"

namespace cvmfs {

/**
 * Counts fetches and, on request, holds every worker inside Fetch() until a
 * given number of them have arrived.  That is what makes the read-ahead's
 * concurrency observable: if only one worker is ever woken, the barrier is
 * never reached and the wait times out.
 */
class BarrierFetcher : public Fetcher {
 public:
  BarrierFetcher(CacheManager *cache_mgr,
                 download::DownloadManager *download_mgr,
                 BackoffThrottle *throttle,
                 perf::StatisticsTemplate statistics)
      : Fetcher(cache_mgr, download_mgr, throttle, statistics)
      , fetches_(0)
      , inside_(0)
      , peak_inside_(0)
      , release_(false) {
    pthread_mutex_init(&lock_, NULL);
    pthread_cond_init(&cond_, NULL);
  }
  virtual ~BarrierFetcher() {
    pthread_cond_destroy(&cond_);
    pthread_mutex_destroy(&lock_);
  }

  int Fetch(const CacheManager::LabeledObject &object,
            const std::string & /* alt_url */ = "") override {
    pthread_mutex_lock(&lock_);
    fetches_++;
    hashes_.push_back(object.id);
    inside_++;
    if (inside_ > peak_inside_)
      peak_inside_ = inside_;
    pthread_cond_broadcast(&cond_);
    while (!release_)
      pthread_cond_wait(&cond_, &lock_);
    inside_--;
    pthread_mutex_unlock(&lock_);
    // A negative descriptor: the mock puts nothing in the cache, so handing
    // back a fabricated one would make the caller Close() an unrelated fd.
    return -1;
  }

  /**
   * Waits until at least n workers are simultaneously inside Fetch(), or the
   * deadline passes.  Returns the peak concurrency actually observed.
   */
  unsigned WaitForConcurrency(unsigned n, unsigned timeout_ms) {
    const unsigned step_ms = 20;
    unsigned waited = 0;
    pthread_mutex_lock(&lock_);
    while ((inside_ < n) && (waited < timeout_ms)) {
      pthread_mutex_unlock(&lock_);
      usleep(step_ms * 1000);
      waited += step_ms;
      pthread_mutex_lock(&lock_);
    }
    const unsigned peak = peak_inside_;
    pthread_mutex_unlock(&lock_);
    return peak;
  }

  void Release() {
    const MutexLockGuard m(&lock_);
    release_ = true;
    pthread_cond_broadcast(&cond_);
  }

  unsigned fetches() {
    const MutexLockGuard m(&lock_);
    return fetches_;
  }

  std::vector<shash::Any> hashes() {
    const MutexLockGuard m(&lock_);
    return hashes_;
  }

 private:
  pthread_mutex_t lock_;
  pthread_cond_t cond_;
  unsigned fetches_;
  unsigned inside_;
  unsigned peak_inside_;
  bool release_;
  std::vector<shash::Any> hashes_;
};


class T_ChunkReadahead : public ::testing::Test {
 protected:
  virtual void SetUp() {
    tmp_path_ = CreateTempDir("./cvmfs_ut_readahead");
    ASSERT_FALSE(tmp_path_.empty());
    cache_mgr_ = PosixCacheManager::Create(tmp_path_, false);
    ASSERT_TRUE(cache_mgr_ != NULL);
    throttle_ = new BackoffThrottle();
    fetcher_ = new BarrierFetcher(
        cache_mgr_, NULL, throttle_,
        perf::StatisticsTemplate("test", &statistics_));
  }

  virtual void TearDown() {
    delete fetcher_;
    delete throttle_;
    delete cache_mgr_;
    if (!tmp_path_.empty())
      RemoveTree(tmp_path_);
  }

  /**
   * A reflist of n chunks with distinct content hashes.
   */
  FileChunkReflist MakeChunks(unsigned n) {
    FileChunkList *list = new FileChunkList();
    for (unsigned i = 0; i < n; ++i) {
      shash::Any hash(shash::kSha1);
      hash.digest[0] = static_cast<unsigned char>(i + 1);
      list->PushBack(FileChunk(hash, i * 1024, 1024));
    }
    owned_lists_.push_back(list);
    return FileChunkReflist(list, PathString("/file"), zlib::kZlibDefault,
                            false);
  }

  void FreeChunks() {
    for (unsigned i = 0; i < owned_lists_.size(); ++i)
      delete owned_lists_[i];
    owned_lists_.clear();
  }

  std::string tmp_path_;
  perf::Statistics statistics_;
  PosixCacheManager *cache_mgr_;
  BackoffThrottle *throttle_;
  BarrierFetcher *fetcher_;
  std::vector<FileChunkList *> owned_lists_;
};


TEST_F(T_ChunkReadahead, SchedulesUpToDepth) {
  ChunkReadahead readahead(3, 2);
  readahead.Spawn();
  FileChunkReflist chunks = MakeChunks(10);

  readahead.Schedule(chunks, 1, fetcher_, false);
  fetcher_->WaitForConcurrency(2, 2000);
  fetcher_->Release();
  readahead.Stop();

  // Depth is three, so chunks 1..3 and no more.
  EXPECT_LE(fetcher_->fetches(), 3U);
  EXPECT_GT(fetcher_->fetches(), 0U);
  FreeChunks();
}


TEST_F(T_ChunkReadahead, WakesEveryWorkerNotJustOne) {
  // The regression: Schedule() used pthread_cond_signal, which wakes a single
  // worker.  That worker then drains the whole batch by itself because the
  // queue is never empty when it loops, so the chunks are fetched one after
  // another and the other threads stay asleep -- exactly the serialisation
  // this class exists to avoid.
  const unsigned kThreads = 4;
  ChunkReadahead readahead(8, kThreads);
  readahead.Spawn();
  FileChunkReflist chunks = MakeChunks(16);

  readahead.Schedule(chunks, 0, fetcher_, false);
  const unsigned peak = fetcher_->WaitForConcurrency(kThreads, 5000);
  fetcher_->Release();
  readahead.Stop();

  EXPECT_EQ(kThreads, peak)
      << "only " << peak << " of " << kThreads
      << " read-ahead workers ran at once; the batch was serialised";
  FreeChunks();
}


TEST_F(T_ChunkReadahead, SkipsChunksAlreadyInFlight) {
  ChunkReadahead readahead(4, 2);
  readahead.Spawn();
  FileChunkReflist chunks = MakeChunks(8);

  // Two identical batches; the second must not re-queue what is still pending.
  readahead.Schedule(chunks, 0, fetcher_, false);
  fetcher_->WaitForConcurrency(2, 2000);
  readahead.Schedule(chunks, 0, fetcher_, false);
  fetcher_->Release();
  readahead.Stop();

  const std::vector<shash::Any> seen = fetcher_->hashes();
  std::vector<shash::Any> unique = seen;
  std::sort(unique.begin(), unique.end());
  unique.erase(std::unique(unique.begin(), unique.end()), unique.end());
  EXPECT_EQ(unique.size(), seen.size()) << "a pending chunk was fetched twice";
  FreeChunks();
}


TEST_F(T_ChunkReadahead, StopWithoutSpawnAndDoubleStopAreSafe) {
  ChunkReadahead readahead(2, 2);
  readahead.Stop();   // never spawned
  readahead.Stop();   // idempotent
  SUCCEED();
}

}  // namespace cvmfs
