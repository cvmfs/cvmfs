/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MOUNTPOINT_H_
#define CVMFS_MOUNTPOINT_H_

#include <pthread.h>
#include <unistd.h>

#include <string>

#include "loader.h"
#include "fuse_listing.h"
#include "util/pointer.h"

class AuthzAttachment;
class AuthzFetcher;
class AuthzSessionManager;
class BackoffThrottle;
namespace cache {
class CacheManager;
}
class ChunkTables;
namespace cvmfs {
class Uuid;
}
namespace download {
class DownloadManager;
}
namespace glue {
class InodeTracker;
}
namespace lru {
class InodeCache;
class Md5PathCache;
class PathCache;
}
class OptionsManager;
namespace perf {
class Counter;
class Statistics;
}


/**
 * Construction of FileSystem and MountPoint can go wrong.  In this case, we'd
 * like to know why.
 */
class MountPointFactory {
 public:
  MountPointFactory() : boot_status_(loader::kFailUnknown) { }
  bool IsValid() { return boot_status_ == loader::kFailOk; }
  loader::Failures boot_status() { return boot_status_; }
  std::string boot_error() { return boot_error_; }

 protected:
  loader::Failures boot_status_;
  std::string boot_error_;
};


/**
 * The FileSystem object initializes cvmfs' global state.  It sets up the cache
 * directory and it can contain multiple mount points.
 *
 * TODO(jblomer): make FileSystem independent from specific repository
 */
class FileSystem : SingleCopy, public MountPointFactory {
 public:
  enum Type {
    kFsFuse = 0,
    kFsLibrary
  };

  struct FileSystemInfo {
    FileSystemInfo() : type(kFsFuse), options_mgr(NULL) { }
    std::string name;
    std::string exe_path;
    Type type;
    OptionsManager *options_mgr;
  };

  static const unsigned kCacheExclusive  = 0x01;
  static const unsigned kCacheShared     = 0x02;
  static const unsigned kCacheAlien      = 0x04;
  static const unsigned kCacheManaged    = 0x08;
  static const unsigned kCacheNfs        = 0x10;
  static const unsigned kCacheNfsHa      = 0x20;
  static const unsigned kCacheNoRename   = 0x40;

  static FileSystem *Create(const FileSystemInfo &fs_info);
  ~FileSystem();

  std::string cache_dir() { return cache_dir_; }
  cache::CacheManager *cache_mgr() { return cache_mgr_; }
  int cache_mode() { return cache_mode_; }
  std::string exe_path() { return exe_path_; }
  bool found_previous_crash() { return found_previous_crash_; }
  std::string nfs_maps_dir() { return nfs_maps_dir_; }
  OptionsManager *options_mgr() { return options_mgr_; }
  int64_t quota_limit() { return quota_limit_; }
  perf::Statistics *statistics() { return statistics_; }
  Type type() { return type_; }
  std::string workspace() { return workspace_; }
  cvmfs::Uuid *uuid_cache() { return uuid_cache_; }

 private:
  static const char *kDefaultCacheBase;  // /var/lib/cvmfs
  static const unsigned kDefaultQuotaLimit = 1024 * 1024 * 1024;

  static void LogSqliteError(void *user_data __attribute__((unused)),
                             int sqlite_extended_error,
                             const char *message);

  FileSystem(const FileSystemInfo &fs_info);

  void SetupLogging();
  void CreateStatistics();
  void SetupSqlite();
  bool SetupWorkspace();
  bool LockWorkspace();
  bool SetupCrashGuard();
  bool CreateCache();
  bool SetupQuotaMgmt();
  bool SetupNfsMaps();

  bool CheckCacheMode();
  void DetermineCacheMode();
  void DetermineCacheDirs();
  void DetermineMountpoint();
  void DetermineWorkspace();

  std::string name_;
  std::string exe_path_;
  Type type_;
  OptionsManager *options_mgr_;

  perf::Counter *n_fs_open_;
  perf::Counter *n_fs_dir_open_;
  perf::Counter *n_fs_lookup_;
  perf::Counter *n_fs_lookup_negative_;
  perf::Counter *n_fs_stat_;
  perf::Counter *n_fs_read_;
  perf::Counter *n_fs_readlink_;
  perf::Counter *n_fs_forget_;
  perf::Counter *n_io_error_;
  perf::Counter *no_open_files_;
  perf::Counter *no_open_dirs_;
  perf::Statistics *statistics_;

  std::string workspace_;
  int fd_workspace_lock_;
  std::string path_workspace_lock_;
  std::string path_crash_guard_;
  bool found_previous_crash_;

  std::string mountpoint_;
  std::string cache_dir_;
  std::string nfs_maps_dir_;
  int cache_mode_;
  int64_t quota_limit_;

  cache::CacheManager *cache_mgr_;
  cvmfs::Uuid *uuid_cache_;
  bool has_nfs_maps_;
  bool has_custom_sqlitevfs_;
};


/**
 * A MountPoint provides a clip around all the different *Manager objects that
 * in combination represent a mounted cvmfs repository.  Its main purpose is
 * the controlled construction and deconstruction of the involved ensemble of
 * classes based on the information passed from an options manager.
 *
 * We use pointers to manager classes to make the order of construction and
 * destruction explicit and also to keep the include list for this header small.
 */
class MountPoint : SingleCopy, public MountPointFactory {
 public:
  static MountPoint *Create(const std::string &fqrn,
                            FileSystem *file_system);
  ~MountPoint();

  unsigned GetMaxTtlMn();
  void SetMaxTtlMn(unsigned value_minutes);

  BackoffThrottle *backoff_throttle() { return backoff_throttle_; }
  perf::Statistics *statistics() { return statistics_; }

 private:
  /**
   * The maximum TTL can be used to cap a root catalogs registered ttl.  By
   * default this is disabled (= 0).
   */
  static const unsigned kDefaultMaxTtlSec = 0;
  /**
   * Let fuse cache dentries for 1 minute.
   */
  static const unsigned kDefaultKCacheTtlSec = 60;
  /**
   * Number of Md5Path entries in the libcvmfs cache.
   */
  static const unsigned kLibPathCacheSize = 32000;
  /**
   * Cache seven times more md5 paths than inodes in the fuse module.
   */
  static const unsigned kInodeCacheFactor = 7;
  /**
   * Default to 16M RAM for meta-data caches; does not include the inode tracker
   */
  static const unsigned kDefaultMemcacheSize = 16 * 1024 * 1024;
  /**
   * Where to look for external authz helpers.
   */
  static const char *kDefaultAuthzSearchPath;  // "/usr/libexec/cvmfs/authz"
  /**
   * Maximum number of concurrent HTTP connections.
   */
  static const unsigned kDefaultNumConnections = 16;
  /**
   * Default network timeout
   */
  static const unsigned kDefaultTimeoutSec = 5;
  static const unsigned kDefaultRetries = 1;
  static const unsigned kDefaultBackoffInitMs = 2000;
  static const unsigned kDefaultBackoffMaxMs = 10000;

  MountPoint(const std::string &fqrn, FileSystem *file_system);

  void CreateStatistics();
  void CreateAuthz();
  bool CreateDownloadManagers();
  void CreateTables();
  void SetupTtls();
  void SetupDnsTuning();
  void SetupHttpTuning();
  std::string ReplaceHosts(std::string hosts);

  std::string fqrn_;
  /**
   * In contrast to the manager objects, the FileSystem is not owned.
   */
  FileSystem *file_system_;

  perf::Statistics *statistics_;
  AuthzFetcher *authz_fetcher_;
  AuthzSessionManager *authz_session_mgr_;
  AuthzAttachment *authz_attachment_;
  download::DownloadManager *download_mgr_;
  download::DownloadManager *external_download_mgr_;
  FuseDirectoryHandles *directory_handles_;
  ChunkTables *chunk_tables_;
  lru::InodeCache *inode_cache_;
  lru::PathCache *path_cache_;
  lru::Md5PathCache *md5path_cache_;
  glue::InodeTracker *inode_tracker_;

  BackoffThrottle *backoff_throttle_;

  unsigned max_ttl_sec_;
  pthread_mutex_t lock_max_ttl_;
  double kcache_timeout_sec_;
};  // class MointPoint

#endif  // CVMFS_MOUNTPOINT_H_
