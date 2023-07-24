/**
 * This file is part of the CernVM File System.
 *
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "cache_posix_refc.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef __APPLE__
#include <sys/statfs.h>
#endif
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include <iostream>

#include "crypto/hash.h"
#include "crypto/signature.h"
#include "directory_entry.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "quota.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/atomic.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

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

  FileSystemInfo fs_info = GetFileSystemInfo(cache_path);

  if (fs_info.type == kFsTypeTmpfs) {
    cache_manager->is_tmpfs_ = true;
  }

  if (cache_manager->alien_cache_) {
    if (!MakeCacheDirectories(cache_path, 0770)) {
      return NULL;
    }
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Cache directory structure created.");
    switch (fs_info.type) {
      case kFsTypeNFS:
        cache_manager->rename_workaround_ = kRenameLink;
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Alien cache is on NFS.");
        break;
      case kFsTypeBeeGFS:
        cache_manager->rename_workaround_ = kRenameSamedir;
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Alien cache is on BeeGFS.");
        break;
      default:
        break;
    }
  } else {
    if (!MakeCacheDirectories(cache_path, 0700))
      return NULL;
  }

  // TODO(jblomer): we might not need to look anymore for cvmfs 2.0 relicts
  if (FileExists(cache_path + "/cvmfscatalog.cache")) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "Not mounting on cvmfs 2.0.X cache");
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
  if (result >= -1) {
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
  assert(data);
  SavedState *state = reinterpret_cast<SavedState *>(data);
  fd_mgr.AssignFrom(state->fd_mgr);
  return -1;
}


bool PosixRefcountCacheManager::DoFreeState(void *data) {
  SavedState *state = reinterpret_cast<SavedState *>(data);
  delete state->fd_mgr;
  delete state;
  return true;
}


