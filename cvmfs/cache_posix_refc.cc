/**
 * This file is part of the CernVM File System.
 *
 */

#include "cvmfs_config.h"
#include "cache_posix_refc.h"

#include "quota.h"
#include "util/posix.h"

using namespace std;  // NOLINT


PosixRefcountCacheManager *PosixRefcountCacheManager::Create(
  const string &cache_path,
  const bool alien_cache,
  const RenameWorkarounds rename_workaround)
{
  UniquePtr<PosixRefcountCacheManager> cache_manager(
    new PosixRefcountCacheManager(cache_path, alien_cache));
  assert(cache_manager.IsValid());

  cache_manager->rename_workaround_ = rename_workaround;

  bool result_ = cache_manager->InitCacheDirectory(cache_path);
  if (!result_) {
    return NULL;
  }

  return cache_manager.Release();
}


int PosixRefcountCacheManager::Close(int fd) {
  int retval = fd_mgr.Close(fd);
  if (retval != 0)
    return -errno;
  return 0;
}

int PosixRefcountCacheManager::Open(const LabeledObject &object) {
  const string path = GetPathInCache(object.id);
  int result = fd_mgr.Open(object.id, path);
  if (result >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit %s", path.c_str());
    // platform_disable_kcache(result);
    quota_mgr_->Touch(object.id);
  } else {
    result = -errno;
    LogCvmfs(kLogCache, kLogDebug, "miss %s (%d)", path.c_str(), result);
  }
  return result;
}


string PosixRefcountCacheManager::Describe() {
  return "Posix cache manager with refcount of file descriptors"
    "(cache directory: " + cache_path_ + ")\n";
}


void *PosixRefcountCacheManager::DoSaveState() {
  SavedState *state = new SavedState();
  state->fd_mgr = fd_mgr.Clone();
  return state;
}


int PosixRefcountCacheManager::DoRestoreState(void *data) {
  fd_mgr = FdRefcountMgr();
  assert(data);
    SavedState *state = reinterpret_cast<SavedState *>(data);
  if (state->magic_number == 32123) {
      LogCvmfs(kLogCache, kLogDebug, "Restoring refcount cache manager from "
                                     "refcounted posix cache manager");

    fd_mgr.AssignFrom(state->fd_mgr);
  } else {
      LogCvmfs(kLogCache, kLogDebug, "Restoring refcount cache manager from "
                                     "non-refcounted posix cache manager");
  }
  return -1;
}


bool PosixRefcountCacheManager::DoFreeState(void *data) {
  assert(data);
  SavedState *state = reinterpret_cast<SavedState *>(data);
  if (state->magic_number == 32123) {
    delete state->fd_mgr;
    delete state;
  } else {
    // this should be the dummy SavedState
    // of the regular posix cache manager
    free(data);
  }
  return true;
}


