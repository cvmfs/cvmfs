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
#include "crypto/hash.h"
#include "duplex_testing.h"
#include "network/download.h"
#include "network/sink.h"

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
  TransactionSink(CacheManager *cache_mgr, void *open_txn)
      : Sink(false), cache_mgr_(cache_mgr), open_txn_(open_txn) { }
  virtual ~TransactionSink() { }

  /**
   * Appends data to the sink
   *
   * @returns on success: number of bytes written (can be less than requested)
   *          on failure: -errno.
   */
  virtual int64_t Write(const void *buf, uint64_t sz) {
    return cache_mgr_->Write(buf, sz, open_txn_);
  }

  /**
   * Truncate all written data and start over at position zero.
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Reset() { return cache_mgr_->Reset(open_txn_); }

  /**
   * Purges all resources leaving the sink in an invalid state.
   * More aggressive version of Reset().
   * For some sinks it might do the same as Reset().
   *
   * @returns Success = 0
   *          Failure = -errno
   */
  virtual int Purge() { return Reset(); }
  /**
   * @returns true if the object is correctly initialized.
   */
  virtual bool IsValid() { return cache_mgr_ != NULL && open_txn_ != NULL; }

  virtual int Flush() { return 0; }
  virtual bool Reserve(size_t /*size*/) { return true; }
  virtual bool RequiresReserve() { return false; }

  /**
   * Return a string representation describing the type of sink and its status
   */
  virtual std::string Describe() {
    std::string result = "Transaction sink that is ";
    result += IsValid() ? "valid" : "invalid";
    return result;
  }

 private:
  CacheManager *cache_mgr_;
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
  Fetcher(CacheManager *cache_mgr,
          download::DownloadManager *download_mgr,
          BackoffThrottle *backoff_throttle,
          perf::StatisticsTemplate statistics);
  ~Fetcher();

  int Fetch(const CacheManager::LabeledObject &object,
            const std::string &alt_url = "");

  void ReplaceCacheManager(CacheManager *new_cache_mgr) {
    cache_mgr_ = new_cache_mgr;
  }
  CacheManager *cache_mgr() { return cache_mgr_; }
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
      fetcher = NULL;
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
  typedef std::map<shash::Any, std::vector<int> *> ThreadQueues;

  ThreadLocalStorage *GetTls();
  void CleanupTls(ThreadLocalStorage *tls);
  void SignalWaitingThreads(const int fd, const shash::Any &id,
                            ThreadLocalStorage *tls);
  int OpenSelect(const CacheManager::LabeledObject &object);

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

  CacheManager *cache_mgr_;
  download::DownloadManager *download_mgr_;
  BackoffThrottle *backoff_throttle_;
  perf::Counter *n_downloads;
  perf::Counter *n_invocations;
};

}  // namespace cvmfs

#endif  // CVMFS_FETCH_H_
