/**
 * This file is part of the CernVM File System.
 *
 * This is the internal implementation of libcvmfs,
 * not to be exposed to the code using the library.
 */

#ifndef CVMFS_LIBCVMFS_INT_H_
#define CVMFS_LIBCVMFS_INT_H_

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

int cvmfs_readlink(const char *path, char *buf, size_t size);
int cvmfs_open(const char *c_path);
int cvmfs_close(int fd);
int cvmfs_statfs(const char *path __attribute__((unused)), struct statvfs *info);
int cvmfs_getattr(const char *c_path, struct stat *info);
int cvmfs_listdir(const char *path,char ***buf,size_t *buflen);

int cvmfs_int_init(
  const std::string &cvmfs_opts_hostname,  // url of repository
  const std::string &cvmfs_opts_proxies,
  const std::string &cvmfs_opts_repo_name,
  const std::string &cvmfs_opts_mountpoint,
  const std::string &cvmfs_opts_pubkey,
  const std::string &cvmfs_opts_cachedir,
  const std::string &cvmfs_opts_alien_cachedir,
  bool cvmfs_opts_cd_to_cachedir,
  int64_t cvmfs_opts_quota_limit,
  int64_t cvmfs_opts_quota_threshold,
  bool cvmfs_opts_rebuild_cachedb,
  bool cvmfs_opts_ignore_signature,
  const std::string &cvmfs_opts_root_hash,
  unsigned cvmfs_opts_timeout,
  unsigned cvmfs_opts_timeout_direct,
  int cvmfs_opts_syslog_level,
  const std::string &cvmfs_opts_logfile,
  const std::string &cvmfs_opts_tracefile,
  const std::string &cvmfs_opts_deep_mount,
  const std::string &cvmfs_opts_blacklist,
  int cvmfs_opts_nofiles,
  bool cvmfs_opts_enable_monitor,
  bool cvmfs_opts_enable_async_downloads
);

void cvmfs_int_spawn();
void cvmfs_int_fini();

}  // namespace cvmfs

#endif  // CVMFS_LIBCVMFS_INT_H_
