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
#include "fd_refcount_mgr.h"
#include "file_chunk.h"
#include "gtest/gtest_prod.h"
#include "manifest_fetch.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/atomic.h"


/**
 * Like the Posix Cache Manager, but deduplicates file descriptors for open
 * files with a reference counter.
 */
class PosixRefcountCacheManager : public PosixCacheManager {
 public:
    static PosixRefcountCacheManager *Create(
      const std::string &cache_path,
      const bool alien_cache,
      const RenameWorkarounds rename_workaround = kRenameNormal);

    PosixRefcountCacheManager(const std::string &cache_path,
      const bool alien_cache)
    : PosixCacheManager(cache_path, alien_cache),
    fd_mgr()
  {
    atomic_init32(&no_inflight_txns_);
  }

  virtual ~PosixRefcountCacheManager() {}

  virtual int Open(const LabeledObject &object);
  virtual int Close(int fd);
  virtual std::string Describe();

 protected:
  struct SavedState {
    SavedState() : version(0), fd_mgr(NULL) { }
    unsigned int version;
    FdRefcountMgr *fd_mgr;
  };

  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  FdRefcountMgr fd_mgr;
};  // class PosixRefcountCacheManager

#endif  // CVMFS_CACHE_POSIX_REFC_H_
