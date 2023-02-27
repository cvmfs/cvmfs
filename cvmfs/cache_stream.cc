/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cache_stream.h"

#include <cstdlib>
#include <string>

#include "util/murmur.hxx"
#include "util/mutex.h"
#include "util/smalloc.h"


namespace {

static uint32_t hasher_int(const int &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

}  // anonymous namespace


StreamingCacheManager::StreamingCacheManager(
  CacheManager *cache_mgr, download::DownloadManager *download_mgr)
  : cache_mgr_(cache_mgr)
  , download_mgr_(download_mgr)
  , min_stream_size_(kMinStreamSize)
{
  lock_fd_table_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_fd_table_, NULL);
  assert(retval == 0);
  fd_table_.Init(16, -1, hasher_int);
}

StreamingCacheManager::~StreamingCacheManager() {
  pthread_mutex_destroy(lock_fd_table_);
  free(lock_fd_table_);
  quota_mgr_ = NULL;  // gets deleted by cache_mgr_
}

std::string StreamingCacheManager::Describe() {
  return "Streaming shim, underlying cache manager:\n" + cache_mgr_->Describe();
}

bool StreamingCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  bool result = cache_mgr_->AcquireQuotaManager(quota_mgr);
  if (result)
    quota_mgr_ = cache_mgr_->quota_mgr();
  return result;
}

int StreamingCacheManager::Open(const BlessedObject &object) {
  int fd_in_cache_mgr = cache_mgr_->Open(object);
  if (fd_in_cache_mgr >= 0) {
    MutexLockGuard lock_guard(lock_fd_table_);
    int fd = fd_table_.size();
    fd_table_.Insert(fd, FdInfo(fd_in_cache_mgr));
    return fd;
  }

  if (fd_in_cache_mgr != -ENOENT)
    return fd_in_cache_mgr;

  if ((object.info.type == kTypeCatalog) || (object.info.type == kTypePinned))
    return -ENOENT;

  MutexLockGuard lock_guard(lock_fd_table_);
  int fd = fd_table_.size();
  fd_table_.Insert(fd, FdInfo(object.id));
  return fd;
}

int StreamingCacheManager::Close(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    bool found = fd_table_.Lookup(fd, &info);
    if (!found)
      return -EBADF;
    fd_table_.Erase(fd);
  }

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Close(info.fd_in_cache_mgr);
  return 0;
}
