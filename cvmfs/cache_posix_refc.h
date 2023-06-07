/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_POSIX_REFC_H_
#define CVMFS_CACHE_POSIX_REFC_H_

#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <string>
#include <vector>

#include "backoff.h"
#include "cache.h"
#include "cache_posix.h"
#include "catalog_mgr.h"
#include "crypto/signature.h"
#include "file_chunk.h"
#include "gtest/gtest_prod.h"
#include "manifest_fetch.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/atomic.h"


// for map of file descriptors; as in kvstore.cc
static inline uint32_t hasher_any(const shash::Any &key) {
  // We'll just do the same thing as hasher_md5, since every hash is at
  // least as large.
  return *const_cast<uint32_t *>(
             reinterpret_cast<const uint32_t *>(key.digest) + 1);
}




/**
 * Cache manager implementation using a file system (cache directory) as a
 * backing storage.
 */
class PosixRefcountCacheManager : public PosixCacheManager {
 public:
    static PosixRefcountCacheManager *Create(
      const std::string &cache_path,
      const bool alien_cache,
      const RenameWorkarounds rename_workaround = kRenameNormal);

    PosixRefcountCacheManager(const std::string &cache_path,
      const bool alien_cache)
    : PosixCacheManager(cache_path, alien_cache)
  {
    atomic_init32(&no_inflight_txns_);
    const shash::Any hash_null;
    map_fd_.Init(16, hash_null, hasher_any);

      lock_cache_refcount_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_cache_refcount_, NULL);
  assert(retval == 0);
  }

  virtual ~PosixRefcountCacheManager() {
    pthread_mutex_destroy(lock_cache_refcount_);
    free(lock_cache_refcount_);
    }

  virtual int Open(const LabeledObject &object);
  virtual int Close(int fd);
  virtual std::string Describe();

 protected:
  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  std::map<int, int> map_refcount_;
  SmallHashDynamic<shash::Any, int> map_fd_;
  pthread_mutex_t *lock_cache_refcount_;
};  // class PosixRefcCacheManager

#endif  // CVMFS_CACHE_POSIX_REFC_H_
