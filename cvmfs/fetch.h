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
#include "util.h"

namespace cache {
class CacheManager;
}

namespace cvmfs {

/**
 * The Fetcher acts as a mediator between a cache manager and a download
 * manager.  It provides a file descriptor to a requested object, which is
 * usable with the cache manager.
 * If the object is not in the cache, it is downloaded and stored in the cache.
 *
 * Concurrent download requests for the same id are collapsed.
 */
class Fetcher : SingleCopy {
  FRIEND_TEST(T_Fetcher, SignalWaitingThreads);
  friend void TLSDestructor(void *data);

 public:
  /**
   * As of 25M, a file is considered a "big file", which means it is dangerous
   * to apply asynchronous semantics.  In particular, the Fetcher cleans up
   * opportunistically before starting to download.
   */
  static const uint64_t kBigFile;

  Fetcher(const std::string &quarantaine_path,
          cache::CacheManager *cache_mgr,
          download::DownloadManager *download_mgr);
  ~Fetcher();
  int Fetch(const shash::Any &id,
            const uint64_t size,
            const std::string &name,
            const cache::CacheManager::ObjectType object_type);

 private:
  /**
   * Multiple threads might want to download the same object at the same time.
   * If that happens, only the first thread performs the download.  The other
   * threads wait on a pipe for a notification from the first thread.
   */
  struct ThreadLocalStorage {
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

  std::string quarantaine_path_;
  cache::CacheManager *cache_mgr_;
  download::DownloadManager *download_mgr_;
};

}  // namespace cvmfs

#endif  // CVMFS_FETCH_H_
