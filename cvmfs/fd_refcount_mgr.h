/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FD_REFCOUNT_MGR_H_
#define CVMFS_FD_REFCOUNT_MGR_H_

#include <string>

#include "cache.h"
#include "smallhash.h"


// for map of file descriptors; as in kvstore.cc
static inline uint32_t hasher_any(const shash::Any &key) {
  // We'll just do the same thing as hasher_md5, since every hash is at
  // least as large.
  return *const_cast<uint32_t *>(reinterpret_cast<const uint32_t *>(key.digest)
                                 + 1);
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
    int refcount;   /// refcount for the times the fd was opened in the cache
    shash::Any id;  /// hash of the object opened through the fd

    FdRefcountInfo() : refcount(-1) { }
  };

  FdRefcountMgr();

  ~FdRefcountMgr();

  FdRefcountMgr(const SmallHashDynamic<int, FdRefcountInfo> &map_refcount,
                const SmallHashDynamic<shash::Any, int> &map_fd);

  void AssignFrom(FdRefcountMgr *other);

  SmallHashDynamic<int, FdRefcountInfo> *GetRefcountMapPtr();

  SmallHashDynamic<shash::Any, int> *GetFdMapPtr();

  int Open(const shash::Any id, const std::string &path);

  int Close(int fd);

  int Dup(int fd);

  FdRefcountMgr *Clone();

 private:
  /**
   * map for fd -> refcount lookups. A backreference
   * to the object id is included in FdRefcountInfo in order
   * to be able to remove the file descriptor from map_fd_.
   */
  SmallHashDynamic<int, FdRefcountInfo> map_refcount_;
  /**
   * map for object id -> fd lookups, used when
   * opening files in the cache. The fd is used as key
   * in the refcount map.
   */
  SmallHashDynamic<shash::Any, int> map_fd_;
  pthread_mutex_t *lock_cache_refcount_;
};

#endif  // CVMFS_FD_REFCOUNT_MGR_H_
