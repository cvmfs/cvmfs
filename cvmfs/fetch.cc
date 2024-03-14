/**
 * This file is part of the CernVM File System.
 */


#include "fetch.h"

#include <unistd.h>

#include "backoff.h"
#include "cache.h"
#include "clientctx.h"
#include "interrupt.h"
#include "network/download.h"
#include "quota.h"
#include "statistics.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace cvmfs {

void TLSDestructor(void *data) {
  Fetcher::ThreadLocalStorage *tls = static_cast<Fetcher::ThreadLocalStorage *>(
      data);
  std::vector<Fetcher::ThreadLocalStorage *> *tls_blocks = &tls->fetcher
                                                                ->tls_blocks_;

  {
    const MutexLockGuard m(tls->fetcher->lock_tls_blocks_);
    for (vector<Fetcher::ThreadLocalStorage *>::iterator
             i = tls_blocks->begin(),
             iEnd = tls_blocks->end();
         i != iEnd;
         ++i) {
      if ((*i) == tls) {
        tls_blocks->erase(i);
        break;
      }
    }
  }
  tls->fetcher->CleanupTls(tls);
}


/**
 * Called when a thread exists, releases a ThreadLocalStorage object and
 * removes the pointer to it from tls_blocks_.
 */
void Fetcher::CleanupTls(ThreadLocalStorage *tls) {
  ClosePipe(tls->pipe_wait);
  delete tls;
}


/**
 * Initialized thread-local storage if called the first time in a new thread.
 */
Fetcher::ThreadLocalStorage *Fetcher::GetTls() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
      pthread_getspecific(thread_local_storage_));
  if (tls != NULL)
    return tls;

  tls = new ThreadLocalStorage();
  tls->fetcher = this;
  MakePipe(tls->pipe_wait);
  tls->download_job.SetCompressed(true);
  tls->download_job.SetProbeHosts(true);
  const int retval = pthread_setspecific(thread_local_storage_, tls);
  assert(retval == 0);

  const MutexLockGuard m(lock_tls_blocks_);
  tls_blocks_.push_back(tls);

  return tls;
}


int Fetcher::Fetch(const CacheManager::LabeledObject &object,
                   const std::string &alt_url) {
  int fd_return;  // Read-only file descriptor that is returned
  int retval;

  perf::Inc(n_invocations);

  // Try to open from local cache
  if ((fd_return = OpenSelect(object)) >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit: %s", object.label.path.c_str());
    return fd_return;
  }

  if (object.id.IsNull()) {
    // This has been seen when trying to load the root catalog signed by an
    // invalid certificate on an empty cache
    // TODO(jblomer): check if still necessary after the catalog reload refactor
    LogCvmfs(kLogCache, kLogDebug, "cancel attempt to download null hash");
    return -EIO;
  }

  ThreadLocalStorage *tls = GetTls();

  // Synchronization point: either act as a master thread for this object or
  // enqueue to the list of waiting threads.
  pthread_mutex_lock(lock_queues_download_);
  const ThreadQueues::iterator iDownloadQueue = queues_download_.find(
      object.id);
  if (iDownloadQueue != queues_download_.end()) {
    LogCvmfs(kLogCache, kLogDebug, "waiting for download of %s",
             object.label.path.c_str());

    iDownloadQueue->second->push_back(tls->pipe_wait[1]);
    pthread_mutex_unlock(lock_queues_download_);
    ReadPipe(tls->pipe_wait[0], &fd_return, sizeof(int));

    LogCvmfs(kLogCache, kLogDebug, "received from another thread fd %d for %s",
             fd_return, object.label.path.c_str());
    return fd_return;
  } else {
    // Seems we are the first one, check again in the cache (race condition)
    fd_return = OpenSelect(object);
    if (fd_return >= 0) {
      pthread_mutex_unlock(lock_queues_download_);
      return fd_return;
    }

    // Create a new queue for this chunk
    queues_download_[object.id] = &tls->other_pipes_waiting;
    pthread_mutex_unlock(lock_queues_download_);
  }

  perf::Inc(n_downloads);

  // Involve the download manager
  LogCvmfs(kLogCache, kLogDebug, "downloading %s", object.label.path.c_str());
  std::string url;
  if (object.label.IsExternal()) {
    url = !alt_url.empty() ? alt_url : object.label.path;
  } else {
    url = "/" + (alt_url.size() ? alt_url : "data/" + object.id.MakePath());
  }
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  retval = cache_mgr_->StartTxn(object.id, object.label.size, txn);
  if (retval < 0) {
    LogCvmfs(kLogCache, kLogDebug, "could not start transaction on %s",
             object.label.path.c_str());
    SignalWaitingThreads(retval, object.id, tls);
    return retval;
  }
  cache_mgr_->CtrlTxn(object.label, 0, txn);

  LogCvmfs(kLogCache, kLogDebug, "miss: %s %s", object.label.path.c_str(),
           url.c_str());
  TransactionSink sink(cache_mgr_, txn);
  tls->download_job.SetUrl(&url);
  tls->download_job.SetSink(&sink);
  tls->download_job.SetExpectedHash(&object.id);
  tls->download_job.SetExtraInfo(&object.label.path);
  ClientCtx *ctx = ClientCtx::GetInstance();
  if (ctx->IsSet()) {
    ctx->Get(tls->download_job.GetUidPtr(),
             tls->download_job.GetGidPtr(),
             tls->download_job.GetPidPtr(),
             tls->download_job.GetInterruptCuePtr());
  } else {
    *(tls->download_job.GetUidPtr()) = -1;
    *(tls->download_job.GetGidPtr()) = -1;
    *(tls->download_job.GetPidPtr()) = -1;
    *(tls->download_job.GetInterruptCuePtr()) = NULL;
  }
  tls->download_job.SetCompressed(object.label.zip_algorithm
                                  == zlib::kZlibDefault);
  tls->download_job.SetRangeOffset(object.label.range_offset);
  tls->download_job.SetRangeSize(static_cast<int64_t>(object.label.size));
  download_mgr_->Fetch(&tls->download_job);

  if (tls->download_job.error_code() == download::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug, "finished downloading of %s", url.c_str());

    fd_return = cache_mgr_->OpenFromTxn(txn);
    if (fd_return < 0) {
      cache_mgr_->AbortTxn(txn);
      SignalWaitingThreads(fd_return, object.id, tls);
      return fd_return;
    }

    retval = cache_mgr_->CommitTxn(txn);
    if (retval < 0) {
      cache_mgr_->Close(fd_return);
      SignalWaitingThreads(retval, object.id, tls);
      return retval;
    }
    SignalWaitingThreads(fd_return, object.id, tls);
    return fd_return;
  }

  // Download failed
  LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
           "failed to fetch %s (hash: %s, error %d [%s])",
           object.label.path.c_str(), object.id.ToString().c_str(),
           tls->download_job.error_code(),
           download::Code2Ascii(tls->download_job.error_code()));
  cache_mgr_->AbortTxn(txn);
  backoff_throttle_->Throttle();
  SignalWaitingThreads(-EIO, object.id, tls);
  return -EIO;
}


Fetcher::Fetcher(CacheManager *cache_mgr,
                 download::DownloadManager *download_mgr,
                 BackoffThrottle *backoff_throttle,
                 perf::StatisticsTemplate statistics)
    : lock_queues_download_(NULL)
    , lock_tls_blocks_(NULL)
    , cache_mgr_(cache_mgr)
    , download_mgr_(download_mgr)
    , backoff_throttle_(backoff_throttle) {
  int retval;
  retval = pthread_key_create(&thread_local_storage_, TLSDestructor);
  assert(retval == 0);
  lock_queues_download_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_queues_download_, NULL);
  assert(retval == 0);
  lock_tls_blocks_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_tls_blocks_, NULL);
  assert(retval == 0);
  n_downloads = statistics.RegisterTemplated(
      "n_downloads",
      "overall number of downloaded files (incl. catalogs, chunks)");
  n_invocations = statistics.RegisterTemplated(
      "n_invocations",
      "overall number of object requests (incl. catalogs, chunks)");
}


Fetcher::~Fetcher() {
  int retval;

  {
    const MutexLockGuard m(lock_tls_blocks_);
    for (unsigned i = 0; i < tls_blocks_.size(); ++i)
      CleanupTls(tls_blocks_[i]);
  }

  retval = pthread_mutex_destroy(lock_tls_blocks_);
  assert(retval == 0);
  free(lock_tls_blocks_);

  retval = pthread_mutex_destroy(lock_queues_download_);
  assert(retval == 0);
  free(lock_queues_download_);

  retval = pthread_key_delete(thread_local_storage_);
  assert(retval == 0);
}


/**
 * Depending on the object type, uses either Open() or OpenPinned() from the
 * cache manager
 */
int Fetcher::OpenSelect(const CacheManager::LabeledObject &object) {
  if (object.label.IsCatalog() || object.label.IsPinned()) {
    return cache_mgr_->OpenPinned(object);
  } else {
    return cache_mgr_->Open(object);
  }
}


void Fetcher::SignalWaitingThreads(const int fd,
                                   const shash::Any &id,
                                   ThreadLocalStorage *tls) {
  const MutexLockGuard m(lock_queues_download_);
  for (unsigned i = 0, s = tls->other_pipes_waiting.size(); i < s; ++i) {
    int fd_dup = (fd >= 0) ? cache_mgr_->Dup(fd) : fd;
    WritePipe(tls->other_pipes_waiting[i], &fd_dup, sizeof(int));
  }
  tls->other_pipes_waiting.clear();
  queues_download_.erase(id);
}

}  // namespace cvmfs
