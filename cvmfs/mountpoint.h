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

class BackoffThrottle;
class ChunkTables;
namespace cvmfs {
class Uuid;
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
 * directory and it can contain mutiple mount points.
 */
class FileSystem : SingleCopy, public MountPointFactory {
 public:
  enum Type {
    kFsFuse = 0,
    kFsLibrary
  };

  static const unsigned kCacheExclusive  = 0x01;
  static const unsigned kCacheShared     = 0x02;
  static const unsigned kCacheAlien      = 0x04;
  static const unsigned kCacheManaged    = 0x08;
  static const unsigned kCacheNfs        = 0x10;
  static const unsigned kCacheNfsHa      = 0x20;
  static const unsigned kCacheNoRename   = 0x40;

  static FileSystem *Create(Type type, OptionsManager *options_manager);
  ~FileSystem();

  OptionsManager *options_manager() { return options_manager_; }
  Type type() { return type_; }

 private:
  static const char *kDefaultCacheBase;  // /var/lib/cvmfs
  static const unsigned kDefaultQuotaLimit = 1024 * 1024 * 1024;

  static void LogSqliteError(void *user_data __attribute__((unused)),
                             int sqlite_extended_error,
                             const char *message);

  FileSystem(Type type, OptionsManager *options_manager);

  void SetupLogging();
  void SetupSqlite();
  bool SetupCache();
  void DetermineCacheMode();
  void DetermineCacheDirs();
  bool CheckCacheMode();

  Type type_;
  OptionsManager *options_manager_;

  std::string fqrn_;
  std::string cache_dir_;
  std::string alien_cache_dir_;
  int cache_mode_;
  int fd_cache_lock_;
  int64_t quota_limit_;
};


/**
 * A MountPoint provides a clip around all the different *Manager objects that
 * in combination represent a mounted cvmfs repository.  It's main purpose is
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

  MountPoint(const std::string &fqrn, FileSystem *file_system);
  void CreateStatistics();
  void CreateTables();
  void SetupTtls();

  std::string fqrn_;
  /**
   * In contrast to the manager objects, the FileSystem is not owned.
   */
  FileSystem *file_system_;
  cvmfs::Uuid *uuid_mountpoint_;

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
