/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FETCH_H_
#define CVMFS_FETCH_H_

#include <pthread.h>

#include <map>
#include <string>
#include <vector>

#include "cache.h"
#include "download.h"
#include "gtest/gtest_prod.h"
#include "hash.h"
#include "sink.h"
#include "util.h"

class BackoffThrottle;

namespace perf {
class Statistics;
}

namespace cvmfs {

/**
 * TransacionSink uses an open transaction in a cache manager as a sink.  It
 * allows the download manager to write data without knowing about the cache
 * manager.
 */
class TransactionSink : public Sink {
 public:
  TransactionSink(cache::CacheManager *cache_mgr, void *open_txn)
    : cache_mgr_(cache_mgr)
    , open_txn_(open_txn)
  { }
  virtual ~TransactionSink() { }
  virtual int64_t Write(const void *buf, uint64_t sz) {
    return cache_mgr_->Write(buf, sz, open_txn_);
  }
  virtual int Reset() {
    return cache_mgr_->Reset(open_txn_);
  }

 private:
  cache::CacheManager *cache_mgr_;
  void *open_txn_;
};


/**
 * The Fetcher uses a cache manager and a download manager in order to provide a
 * (virtual) file descriptor to a requested object, which is valid in the
 * context of the cache manager.
 * If the object is not in the cache, it is downloaded and stored in the cache.
 *
 * Concurrent download requests for the same id are collapsed.
 */
class Fetcher : SingleCopy {
  FRIEND_TEST(T_Fetcher, GetTls);
  FRIEND_TEST(T_Fetcher, SignalWaitingThreads);
  friend void *TestGetTls(void *data);
  friend void *TestFetchCollapse(void *data);
  friend void *TestFetchCollapse2(void *data);
  friend void TLSDestructor(void *data);

 public:
  Fetcher(cache::CacheManager *cache_mgr,
          download::DownloadManager *download_mgr,
          BackoffThrottle *backoff_throttle,
          perf::Statistics *statistics,
          const std::string &name = "fetch",
          bool external_data = false);
  ~Fetcher();
  // TODO(jblomer): reduce number of arguments
  int Fetch(const shash::Any &id,
            const uint64_t size,
            const std::string &name,
            const zlib::Algorithms compression_algorithm,
            const cache::CacheManager::ObjectType object_type,
            const std::string &alt_url = "",
            off_t range_offset = -1);

  cache::CacheManager *cache_mgr() { return cache_mgr_; }
  download::DownloadManager *download_mgr() { return download_mgr_; }

 private:
  /**
   * Multiple threads might want to download the same object at the same time.
   * If that happens, only the first thread performs the download.  The other
   * threads wait on a pipe for a notification from the first thread.
   */
  struct ThreadLocalStorage {
    ThreadLocalStorage() {
      pipe_wait[0] = -1;
      pipe_wait[1] = -1;
    }

    /**
     * Used during cleanup to find tls_blocks_.
     */
    Fetcher *fetcher;
    /**
     * Wait on the reading end if another thread is already downloading the same
     * object.
     */
    int pipe_wait[2];
    /**
     * Writer ends of all the pipes of threads that want to download the same
     * object.
     */
    std::vector<int> other_pipes_waiting;
    /**
     * It is sufficient to construct the JobInfo object once per thread, not
     * on every call to Fetch().
     */
    download::JobInfo download_job;
  };

  /**
   * Maps currently downloaded chunks to the other_pipes_waiting member of the
   * thread local storage of the downloading thread.  This way, a thread can
   * enqueue itself to such an other_pipes_waiting list and gets informed when
   * the download is completed.
   */
  typedef std::map< shash::Any, std::vector<int> * > ThreadQueues;

  ThreadLocalStorage *GetTls();
  void CleanupTls(ThreadLocalStorage *tls);
  void SignalWaitingThreads(const int fd, const shash::Any &id,
                            ThreadLocalStorage *tls);
  int OpenSelect(const shash::Any &id,
                 const std::string &name,
                 const cache::CacheManager::ObjectType object_type);

  /**
   * If set to true, this fetcher is in 'external data' mode:
   * instead of constructing the to-be-downloaded URL from the entry hash,
   * it will use the filename.
   */
  bool external_;

  /**
   * Key to the thread's ThreadLocalStorage memory
   */
  pthread_key_t thread_local_storage_;

  ThreadQueues queues_download_;
  pthread_mutex_t *lock_queues_download_;

  /**
   * All the threads register their thread local storage here, so that it can
   * be cleaned up properly in the destructor of Fetcher.
   */
  std::vector<ThreadLocalStorage *> tls_blocks_;
  pthread_mutex_t *lock_tls_blocks_;

  cache::CacheManager *cache_mgr_;
  download::DownloadManager *download_mgr_;
  BackoffThrottle *backoff_throttle_;
  perf::Counter *n_downloads;
};

}  // namespace cvmfs

#endif  // CVMFS_FETCH_H_
