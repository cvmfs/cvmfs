/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CVMFS_H_
#define CVMFS_CVMFS_H_

#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "catalog_mgr.h"
#include "lru.h"

namespace cvmfs {

extern pid_t pid_;
extern std::string *mountpoint_;
extern std::string *repository_name_;
extern int max_cache_timeout_;
extern bool foreground_;
extern bool nfs_maps_;

int ClearFile(const std::string &path);
catalog::LoadError RemountStart();
unsigned GetRevision();
std::string GetOpenCatalogs();
unsigned GetMaxTTL();  // in minutes
void SetMaxTTL(const unsigned value);  // in minutes
void ResetErrorCounters();
void GetLruStatistics(lru::Statistics *inode_stats, lru::Statistics *path_stats,
                      lru::Statistics *md5path_stats);
catalog::Statistics GetCatalogStatistics();
std::string GetCertificateStats();
std::string GetFsStats();

}  // namespace cvmfs

#endif  // CVMFS_CVMFS_H_
