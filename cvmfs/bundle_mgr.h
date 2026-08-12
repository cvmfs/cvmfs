/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_MGR_H_
#define CVMFS_BUNDLE_MGR_H_

#include <limits.h>
#include <pthread.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <vector>

#include "duplex_testing.h"
#include "file_bundle.h"
#include "mountpoint.h"
#include "shortstring.h"
#include "util/posix.h"
#include "util/single_copy.h"

class MockFetcher;

/**
 * Long-lived, best-effort prefetcher for file bundles. One instance per
 * mount point (created when CVMFS_PREFETCH_FILEBUNDLES is on) owns a
 * dispatcher thread and a pool of fetcher threads for the lifetime of the
 * mount. open() calls of a bundle trigger merely enqueue the trigger path
 * via ScheduleTrigger() and return; spec loading and dependency downloads
 * happen entirely on the background threads.
 *
 * The constructor only sets up the bounded queues; the threads are started
 * by Spawn(). The fuse client daemonizes between initialization and
 * cvmfs::Spawn(), and threads do not survive the fork, so Spawn() must not
 * be called before then. libcvmfs never forks and calls Spawn() right after
 * creating the mount point.
 */
class BundleMgr : SingleCopy {
  friend class T_BundleMgr;
  FRIEND_TEST(T_BundleMgr, ExchangeCT);
  FRIEND_TEST(T_BundleMgr, ExchangePathString);
  FRIEND_TEST(T_BundleMgr, ReceivePathLongerThanPipeBuf);
  FRIEND_TEST(T_BundleMgr, EnqueueDependencies);
  FRIEND_TEST(T_BundleMgr, FetchChunked);
  FRIEND_TEST(T_BundleMgr, ScheduleTrigger);
  FRIEND_TEST(T_BundleMgr, ScheduleTriggerBeforeSpawn);
  FRIEND_TEST(T_BundleMgr, TrySendPathDropsWhenFull);

 public:
  explicit BundleMgr(MountPoint *mp);
  virtual ~BundleMgr() {
    atomic_write32(&terminating_, 1);
    JoinDispatcher();
    JoinFetcherPool();
    pthread_mutex_destroy(&worker_read_mutex_);
  }

  /**
   * Starts the dispatcher thread and the fetcher pool. Triggers scheduled
   * before Spawn() wait in the queue until the threads come up.
   */
  void Spawn();

  /**
   * Hands a trigger file over to the background prefetcher. Never blocks:
   * if the trigger queue is full the request is dropped (prefetching is
   * best-effort). Returns whether the trigger was enqueued.
   */
  bool ScheduleTrigger(const PathString &path);
  explicit operator bool() const { return is_valid_; }

 private:
  static void *MainBundleMgrFetcher(void *data);
  static void *MainBundleMgrDispatcher(void *data);
  void SpawnFetcherPool();
  void JoinFetcherPool();
  void SpawnDispatcher();
  void JoinDispatcher();
  void ProcessTrigger(const PathString &trigger_path);
  void EnqueueDependencies(BundleFileMgr *bfm, const PathString &parent_path);
  PathString ReceivePath(int fd) const;
  bool TrySendPath(int fd, const PathString &path) const;
  static PathString NormalizeDependencyPath(const PathString &path,
                                            const PathString &parent_path);

  void FetchPath(const PathString &path);

  // CT stands for contiguous type
  template<typename CT,
           typename = typename std::enable_if<
               std::is_trivially_copyable<CT>::value>::type>
  void BlockingSend(int fd, const CT &obj, size_t size = sizeof(CT)) const {
    typedef typename std::remove_cv<CT>::type T;
    static_assert(
        std::is_trivially_copyable<T>::value,
        "Can't directly send non trivially copyable types over a pipe");
    static_assert(sizeof(T) == sizeof(CT), "CT illformed");
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");

    const T *ptr = reinterpret_cast<const T *>(&obj);
    WritePipe(fd, ptr, size);
  }

  void BlockingSend(int fd, const PathString &path) const {
    const size_t size = path.GetLength();
    BlockingSend(fd, size);
    WritePipe(fd, path.GetChars(), size * sizeof(char));
  }

  void BlockingSend(int fd, const std::string &string) const {
    const size_t size = string.size();
    BlockingSend(fd, size);
    WritePipe(fd, string.data(), size * sizeof(char));
  }

  template<typename CT,
           typename = typename std::enable_if<
               std::is_trivially_copyable<CT>::value>::type>
  CT BlockingReceive(int fd) const {
    typedef typename std::remove_cv<CT>::type T;
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");
    CT item;
    ReadPipe(fd, static_cast<void *>(&item), sizeof(CT));
    return item;
  }

  std::string BlockingReceive(int fd) const {
    const size_t size = BlockingReceive<size_t>(fd);
    std::string result(size, '\t');
    ReadPipe(fd, static_cast<void *>(&result[0]), size * sizeof(char));
    return result;
  }

  MountPoint *mount_point_;

  // Pool of fetcher threads. All workers share pipe_bm_[0] (read end)
  // and serialize their reads via worker_read_mutex_ so cmd+payload
  // pairs are received atomically.
  std::vector<std::unique_ptr<pthread_t> > fetcher_threads_;
  std::unique_ptr<pthread_t> dispatcher_thread_;
  pthread_mutex_t worker_read_mutex_;
  size_t pool_size_;

  enum class Command {
    kTerminate,
    kFetch
  };

  /**
   * Dependency work queue (a pipe with a non-blocking write end). The
   * dispatcher writes Command + path payload to pipe_bm_[1]; workers read
   * from pipe_bm_[0] under worker_read_mutex_.
   */
  int pipe_bm_[2];
  /**
   * Trigger queue (a pipe with a non-blocking write end). ScheduleTrigger()
   * writes Command + path payload to pipe_triggers_[1]; the dispatcher
   * reads from pipe_triggers_[0].
   */
  int pipe_triggers_[2];
  /**
   * Set on destruction: queued work is drained but no longer processed, so
   * that unmounting does not wait for pending downloads.
   */
  std::atomic<int32_t> terminating_;
  bool is_valid_ = true;
};
#endif  // CVMFS_BUNDLE_MGR_H_
