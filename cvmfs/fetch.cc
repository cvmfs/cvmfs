/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "fetch.h"

#include <unistd.h>

#include "backoff.h"
#include "cache.h"
#include "download.h"
#include "logging.h"
#include "quota.h"
#include "statistics.h"
#include "util.h"

using namespace std;  // NOLINT

namespace cvmfs {

void TLSDestructor(void *data) {
  Fetcher::ThreadLocalStorage *tls =
    static_cast<Fetcher::ThreadLocalStorage *>(data);
  std::vector<Fetcher::ThreadLocalStorage *> *tls_blocks =
    &tls->fetcher->tls_blocks_;

  pthread_mutex_lock(tls->fetcher->lock_tls_blocks_);
  for (vector<Fetcher::ThreadLocalStorage *>::iterator i =
       tls_blocks->begin(), iEnd = tls_blocks->end(); i != iEnd; ++i)
  {
    if ((*i) == tls) {
      tls_blocks->erase(i);
      break;
    }
  }
  pthread_mutex_unlock(tls->fetcher->lock_tls_blocks_);
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
  tls->download_job.destination = download::kDestinationSink;
  tls->download_job.compressed = true;
  tls->download_job.probe_hosts = true;
  int retval = pthread_setspecific(thread_local_storage_, tls);
  assert(retval == 0);
  pthread_mutex_lock(lock_tls_blocks_);
  tls_blocks_.push_back(tls);
  pthread_mutex_unlock(lock_tls_blocks_);
  return tls;
}


int Fetcher::Fetch(
  const shash::Any &id,
  const uint64_t size,
  const std::string &name,
  const cache::CacheManager::ObjectType object_type,
  const std::string &alt_url)
{
  int fd_return;  // Read-only file descriptor that is returned
  int retval;

  // Try to open from local cache
  if ((fd_return = OpenSelect(id, name, object_type)) >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit: %s", name.c_str());
    return fd_return;
  }

  ThreadLocalStorage *tls = GetTls();

  // Synchronization point: either act as a master thread for this object or
  // enqueue to the list of waiting threads.
  pthread_mutex_lock(lock_queues_download_);
  ThreadQueues::iterator iDownloadQueue = queues_download_.find(id);
  if (iDownloadQueue != queues_download_.end()) {
    LogCvmfs(kLogCache, kLogDebug, "waiting for download of %s", name.c_str());

    iDownloadQueue->second->push_back(tls->pipe_wait[1]);
    pthread_mutex_unlock(lock_queues_download_);
    ReadPipe(tls->pipe_wait[0], &fd_return, sizeof(int));

    LogCvmfs(kLogCache, kLogDebug, "received from another thread fd %d for %s",
             fd_return, name.c_str());
    return fd_return;
  } else {
    // Seems we are the first one, check again in the cache (race condition)
    fd_return = OpenSelect(id, name, object_type);
    if (fd_return >= 0) {
      pthread_mutex_unlock(lock_queues_download_);
      return fd_return;
    }

    // Create a new queue for this chunk
    queues_download_[id] = &tls->other_pipes_waiting;
    pthread_mutex_unlock(lock_queues_download_);
  }

  perf::Inc(n_downloads);

  // Involve the download manager
  LogCvmfs(kLogCache, kLogDebug, "downloading %s", name.c_str());
  const string url = "/" + (alt_url.size() ? alt_url : "data/" + id.MakePath());
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  retval = cache_mgr_->StartTxn(id, size, txn);
  if (retval < 0) {
    LogCvmfs(kLogCache, kLogDebug, "could not start transaction on %s",
             name.c_str());
    SignalWaitingThreads(retval, id, tls);
    return retval;
  }
  cache_mgr_->CtrlTxn(name, object_type, 0, txn);

  LogCvmfs(kLogCache, kLogDebug, "miss: %s %s", name.c_str(), url.c_str());
  TransactionSink sink(cache_mgr_, txn);
  tls->download_job.url = &url;
  tls->download_job.destination_sink = &sink;
  tls->download_job.expected_hash = &id;
  tls->download_job.extra_info = &name;
  download_mgr_->Fetch(&tls->download_job);

  if (tls->download_job.error_code == download::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug, "finished downloading of %s", url.c_str());

    fd_return = cache_mgr_->OpenFromTxn(txn);
    if (fd_return < 0) {
      cache_mgr_->AbortTxn(txn);
      SignalWaitingThreads(fd_return, id, tls);
      return fd_return;
    }

    retval = cache_mgr_->CommitTxn(txn);
    if (retval < 0) {
      cache_mgr_->Close(fd_return);
      SignalWaitingThreads(retval, id, tls);
      return retval;
    }
    SignalWaitingThreads(fd_return, id, tls);
    return fd_return;
  }

  // Download failed
  LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
           "failed to fetch %s (hash: %s, error %d [%s])", name.c_str(),
           id.ToString().c_str(), tls->download_job.error_code,
           download::Code2Ascii(tls->download_job.error_code));
  cache_mgr_->AbortTxn(txn);
  backoff_throttle_->Throttle();
  SignalWaitingThreads(-EIO, id, tls);
  return -EIO;
}


Fetcher::Fetcher(
  cache::CacheManager *cache_mgr,
  download::DownloadManager *download_mgr,
  BackoffThrottle *backoff_throttle,
  perf::Statistics *statistics)
  : lock_queues_download_(NULL)
  , lock_tls_blocks_(NULL)
  , cache_mgr_(cache_mgr)
  , download_mgr_(download_mgr)
  , backoff_throttle_(backoff_throttle)
{
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
  n_downloads = statistics->Register("fetch.n_downloads",
    "overall number of downloaded files (incl. catalogs, chunks)");
}


Fetcher::~Fetcher() {
  int retval;

  pthread_mutex_lock(lock_tls_blocks_);
  for (unsigned i = 0; i < tls_blocks_.size(); ++i)
    CleanupTls(tls_blocks_[i]);
  pthread_mutex_unlock(lock_tls_blocks_);

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
int Fetcher::OpenSelect(
  const shash::Any &id,
  const std::string &name,
  const cache::CacheManager::ObjectType object_type)
{
  bool is_catalog = object_type == cache::CacheManager::kTypeCatalog;
  if (is_catalog || (object_type == cache::CacheManager::kTypePinned)) {
    return cache_mgr_->OpenPinned(id, name, is_catalog);
  } else {
    return cache_mgr_->Open(id);
  }
}


void Fetcher::SignalWaitingThreads(
  const int fd,
  const shash::Any &id,
  ThreadLocalStorage *tls)
{
  pthread_mutex_lock(lock_queues_download_);
  for (unsigned i = 0, s = tls->other_pipes_waiting.size(); i < s; ++i) {
    int fd_dup = (fd >= 0) ? cache_mgr_->Dup(fd) : fd;
    WritePipe(tls->other_pipes_waiting[i], &fd_dup, sizeof(int));
  }
  tls->other_pipes_waiting.clear();
  queues_download_.erase(id);
  pthread_mutex_unlock(lock_queues_download_);
}

}  // namespace cvmfs
