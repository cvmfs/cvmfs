/**
 * This file is part of the CernVM File System.
 */

#include "fd_refcount_mgr.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <string>

#include "util/mutex.h"
#include "util/smalloc.h"


int FdRefcountMgr::Open(const shash::Any id, const std::string &path) {
  int result = -1;
  const MutexLockGuard lock_guard(lock_cache_refcount_);
  if (!map_fd_.Lookup(id, &result)) {
    result = open(path.c_str(), O_RDONLY);
    if (result >= 0) {
      map_fd_.Insert(id, result);
    }
  }
  if (result >= 0) {
    FdRefcountInfo refc_info;
    if (map_refcount_.Lookup(result, &refc_info)) {
      refc_info.refcount++;
    } else {
      refc_info.refcount = 1;
      refc_info.id = id;
    }
    map_refcount_.Insert(result, refc_info);
  }
  return result;
}

int FdRefcountMgr::Close(int fd) {
  int retval = -1;
  const MutexLockGuard lock_guard(lock_cache_refcount_);
  FdRefcountInfo refc_info;
  if (map_refcount_.Lookup(fd, &refc_info)) {
    if (refc_info.refcount > 1) {
      refc_info.refcount -= 1;
      map_refcount_.Insert(fd, refc_info);
      retval = 0;
    } else {
      retval = close(fd);
      map_fd_.Erase(refc_info.id);
      map_refcount_.Erase(fd);
    }
  } else {
    // fd not present in our table - this should only happen
    // when reloading from the normal posix cache manager!
    LogCvmfs(kLogCache, kLogDebug,
             "WARNING: trying to close fd that "
             " is not in refcount tables");
    retval = close(fd);
  }
  return retval;
}

int FdRefcountMgr::Dup(int fd) {
  int retval = -1;
  const MutexLockGuard lock_guard(lock_cache_refcount_);
  FdRefcountInfo refc_info;
  if (map_refcount_.Lookup(fd, &refc_info)) {
    refc_info.refcount += 1;
    map_refcount_.Insert(fd, refc_info);
    retval = fd;
  } else {
    // fd not present in our table - this should
    // not happen in the current usage of Dup
    LogCvmfs(kLogCache, kLogDebug,
             "WARNING: trying to dup fd that "
             " is not in refcount tables");
    retval = dup(fd);
  }
  return retval;
}

FdRefcountMgr *FdRefcountMgr::Clone() {
  FdRefcountMgr *clone = new FdRefcountMgr(map_refcount_, map_fd_);
  return clone;
}

SmallHashDynamic<shash::Any, int> *FdRefcountMgr::GetFdMapPtr() {
  return &map_fd_;
}

SmallHashDynamic<int, FdRefcountMgr::FdRefcountInfo> *
FdRefcountMgr::GetRefcountMapPtr() {
  return &map_refcount_;
}

void FdRefcountMgr::AssignFrom(FdRefcountMgr *other) {
  map_fd_ = *other->GetFdMapPtr();
  map_refcount_ = *other->GetRefcountMapPtr();
}

FdRefcountMgr::~FdRefcountMgr() {
  pthread_mutex_destroy(lock_cache_refcount_);
  free(lock_cache_refcount_);
}

FdRefcountMgr::FdRefcountMgr() {
  const shash::Any hash_null;
  map_fd_.Init(16, hash_null, hasher_any);
  map_refcount_.Init(16, -1, hasher_int);
  lock_cache_refcount_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_cache_refcount_, NULL);
  assert(retval == 0);
}

FdRefcountMgr::FdRefcountMgr(
    const SmallHashDynamic<int, FdRefcountMgr::FdRefcountInfo> &map_refcount,
    const SmallHashDynamic<shash::Any, int> &map_fd) {
  const shash::Any hash_null;
  map_fd_.Init(16, hash_null, hasher_any);
  map_refcount_.Init(16, -1, hasher_int);
  map_refcount_ = map_refcount;
  map_fd_ = map_fd;
  lock_cache_refcount_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_cache_refcount_, NULL);
  assert(retval == 0);
}
