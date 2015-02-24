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

namespace download {
class DownloadManager;
}

namespace cvmfs {

extern const loader::LoaderExports *loader_exports_;
extern pid_t pid_;
extern std::string *mountpoint_;
extern std::string *repository_name_;
extern download::DownloadManager *download_manager_;
extern int max_cache_timeout_;
extern bool foreground_;
extern bool nfs_maps_;

bool Evict(const std::string &path);
bool Pin(const std::string &path);
catalog::LoadError RemountStart();
void GetReloadStatus(bool *drainout_mode, bool *maintenance_mode);
unsigned GetRevision();
std::string GetOpenCatalogs();
unsigned GetMaxTTL();  // in minutes
void SetMaxTTL(const unsigned value);  // in minutes
void ResetErrorCounters();
void GetLruStatistics(lru::Statistics *inode_stats, lru::Statistics *path_stats,
                      lru::Statistics *md5path_stats);
std::string PrintInodeTrackerStatistics();
std::string PrintInodeGeneration();
catalog::Statistics GetCatalogStatistics();
std::string GetCertificateStats();
std::string GetFsStats();

}  // namespace cvmfs

#endif  // CVMFS_CVMFS_H_
