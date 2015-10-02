/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CVMFS_H_
#define CVMFS_CVMFS_H_

#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "catalog_mgr.h"
#include "loader.h"
#include "lru.h"
#include "util.h"

namespace cache {
class CacheManager;
}

namespace download {
class DownloadManager;
}

namespace perf {
class Statistics;
}

namespace glue {
class InodeTracker;
}


namespace cvmfs {

extern const loader::LoaderExports *loader_exports_;
extern pid_t pid_;
extern std::string *mountpoint_;
extern std::string *repository_name_;
extern download::DownloadManager *download_manager_;
extern download::DownloadManager *external_download_manager_;
extern cache::CacheManager *cache_manager_;
extern int max_cache_timeout_;
extern bool foreground_;
extern bool nfs_maps_;
extern perf::Statistics *statistics_;
extern glue::InodeTracker *inode_tracker_;

bool Evict(const std::string &path);
bool Pin(const std::string &path);
catalog::LoadError RemountStart();
void GetReloadStatus(bool *drainout_mode, bool *maintenance_mode);
unsigned GetRevision();
std::string GetOpenCatalogs();
unsigned GetMaxTTL();  // in minutes
void SetMaxTTL(const unsigned value);  // in minutes
void ResetErrorCounters();
void UnregisterQuotaListener();

}  // namespace cvmfs

#endif  // CVMFS_CVMFS_H_
