/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FD_REFCOUNT_MGR_H_
#define CVMFS_FD_REFCOUNT_MGR_H_

#include <string>
#include <utility>

#include "cache.h"
#include "smallhash.h"


// for map of file descriptors; as in kvstore.cc
static inline uint32_t hasher_any(const shash::Any &key) {
  // We'll just do the same thing as hasher_md5, since every hash is at
  // least as large.
  return *const_cast<uint32_t *>(
             reinterpret_cast<const uint32_t *>(key.digest) + 1);
}
static inline uint32_t hasher_int(const int &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

class FdRefcountMgr {
 public:
  /**
   * Helper class containing the values for the map: fd -> refcount+id
  */
  struct FdRefcountInfo {
    int refcount;  /// refcount for the times the fd was opened in the cache
    shash::Any id;  /// hash of the object opened through the fd
  };

  FdRefcountMgr() {
    const shash::Any hash_null;
    map_fd_.Init(16, hash_null, hasher_any);
    map_refcount_.Init(16, 0, hasher_int);
    lock_cache_refcount_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
    int retval = pthread_mutex_init(lock_cache_refcount_, NULL);
    assert(retval == 0);
  }

  FdRefcountMgr(
        SmallHashDynamic<int, FdRefcountInfo> map_refcount,
        SmallHashDynamic<shash::Any, int> map_fd):
        map_refcount_(map_refcount), map_fd_(map_fd) {
    const shash::Any hash_null;
    map_fd_.Init(16, hash_null, hasher_any);
    map_refcount_.Init(16, 0, hasher_int);
    lock_cache_refcount_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
    int retval = pthread_mutex_init(lock_cache_refcount_, NULL);
    assert(retval == 0);
  }

  ~FdRefcountMgr() {
    pthread_mutex_destroy(lock_cache_refcount_);
    free(lock_cache_refcount_);
  }

  void AssignFrom(const FdRefcountMgr *other) {
    map_fd_ = other->GetFdMap();
    map_refcount_ = other->GetRefcountMap();
  }

  SmallHashDynamic<int, FdRefcountInfo> GetRefcountMap() const {
    return map_refcount_;
  }

  SmallHashDynamic<shash::Any, int>  GetFdMap() const {
    return map_fd_;
  }

  int Open(const shash::Any id, const std::string path) {
    int result = -1;
    {
    MutexLockGuard lock_guard(lock_cache_refcount_);
    if (!map_fd_.Contains(id)) {
      result = open(path.c_str(), O_RDONLY);
      if (result >= 0) {
        map_fd_.Insert(id, result);
      }
    } else {
      map_fd_.Lookup(id, &result);
    }
    if (result >= 0) {
       FdRefcountInfo* refc_info = new FdRefcountInfo();
      if (map_refcount_.Lookup(result, refc_info)) {
        refc_info->refcount++;
        map_refcount_.Insert(result, *refc_info);
        } else {
        refc_info->refcount = 1;
        refc_info->id = id;
        map_refcount_.Insert(result, *refc_info);
      }
    }
    }
    return result;
    }

  int Close(int fd) {
    int retval = -1;
    {
      MutexLockGuard lock_guard(lock_cache_refcount_);
      FdRefcountInfo* refc_info = new FdRefcountInfo();
      if (map_refcount_.Lookup(fd, refc_info)) {
        if (refc_info->refcount > 0) {
          refc_info->refcount -= 1;
          map_refcount_.Insert(fd, *refc_info);
          retval = 0;
        } else {
          retval = close(fd);
          map_fd_.Erase(refc_info->id);
          map_refcount_.Erase(fd);
        }
    } else {
        retval = close(fd);
    }
  }
  return retval;
  }

  FdRefcountMgr* Clone() {
    FdRefcountMgr* clone = new FdRefcountMgr(map_refcount_, map_fd_);
    return clone;
  }

 private:
  SmallHashDynamic<int, FdRefcountInfo> map_refcount_;
  SmallHashDynamic<shash::Any, int> map_fd_;
  pthread_mutex_t *lock_cache_refcount_;
};

#endif  // CVMFS_FD_REFCOUNT_MGR_H_
