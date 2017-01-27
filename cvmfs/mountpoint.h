/**
 * This file is part of the CernVM File System.
 *
 * Steers the booting of CernVM-FS repositories.
 */

#ifndef CVMFS_MOUNTPOINT_H_
#define CVMFS_MOUNTPOINT_H_

#include <pthread.h>
#include <unistd.h>

#include <ctime>
#include <string>

#include "cache.h"
#include "hash.h"
#include "loader.h"
#include "util/pointer.h"

class AuthzAttachment;
class AuthzFetcher;
class AuthzSessionManager;
class BackoffThrottle;
class CacheManager;
namespace catalog {
class ClientCatalogManager;
class InodeGenerationAnnotation;
}
struct ChunkTables;
namespace cvmfs {
class Fetcher;
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
namespace signature {
class SignatureManager;
}
class SimpleChunkTables;
class Tracer;


/**
 * Construction of FileSystem and MountPoint can go wrong.  In this case, we'd
 * like to know why.  This is a base class for both FileSystem and MountPoint.
 */
class BootFactory {
 public:
  BootFactory() : boot_status_(loader::kFailUnknown) { }
  bool IsValid() { return boot_status_ == loader::kFailOk; }
  loader::Failures boot_status() { return boot_status_; }
  std::string boot_error() { return boot_error_; }

  /**
   * Used in the fuse module to artificially set boot errors that are specific
   * to the fuse boot procedure.
   */
  void set_boot_status(loader::Failures code) { boot_status_ = code; }

 protected:
  loader::Failures boot_status_;
  std::string boot_error_;
};


/**
 * The FileSystem object initializes cvmfs' global state.  It sets up sqlite and
 * the cache directory and it can contain multiple mount points.  It currently
 * does so only for libcvmfs; the cvmfs fuse module has exactly one FileSystem
 * object and one MountPoint object.
 */
class FileSystem : SingleCopy, public BootFactory {
 public:
  enum Type {
    kFsFuse = 0,
    kFsLibrary
  };

  struct FileSystemInfo {
    FileSystemInfo()
      : type(kFsFuse)
      , options_mgr(NULL)
      , wait_workspace(false)
      , foreground(false) { }
    /**
     * Name can is used to identify this particular instance of cvmfs in the
     * cache (directory).  Normally it is the fully qualified repository name.
     * For libcvmfs and in other special mount conditions, it can be something
     * else.  Only file systems with different names can share a cache because
     * the name is part of a lock file.
     */
    std::string name;

    /**
     * Used to fork & execve into different flavors of the binary, e.g. the
     * quota manager.
     */
    std::string exe_path;

    /**
     * Fuse mount point or libcvmfs.
     */
    Type type;

    /**
     * All further configuration has to be present in the options manager.
     */
    OptionsManager *options_mgr;

    /**
     * Decides if FileSystem construction should block if the workspace is
     * currently taken.  This is used to coordinate fuse mounts where the next
     * mount happens while the previous fuse module is not yet fully cleaned
     * up.
     */
    bool wait_workspace;
    /**
     * The fuse module should not daemonize.  That means the quota manager
     * should not daemonize, too, but print debug messages to stdout.
     */
    bool foreground;
  };

  /**
   * Local hard disk cache, no shared quota manager.
   */
  static const unsigned kCacheExclusive  = 0x01;
  /**
   * Connect to the quota manager process for a shared local hard disk cache.
   */
  static const unsigned kCacheShared     = 0x02;
  /**
   * Unmanaged cache (no cleanup), cache directory writable
   * for owner uid _and_ gid
   */
  static const unsigned kCacheAlien      = 0x04;
  /**
   * Whether to use a quota manager or not.
   */
  static const unsigned kCacheManaged    = 0x08;
  /**
   * Use NFS maps to persistently map paths to inodes.
   */
  static const unsigned kCacheNfs        = 0x10;
  /**
   * Store NFS maps on an NFS volume.
   */
  static const unsigned kCacheNfsHa      = 0x20;
  /**
   * Avoid rename() calls in the cache directory (replaced by link+unlink)
   */
  static const unsigned kCacheNoRename   = 0x40;

  static FileSystem *Create(const FileSystemInfo &fs_info);
  ~FileSystem();

  bool IsNfsSource() { return cache_mode_ & kCacheNfs; }
  bool IsAlienCache() { return cache_mode_ & kCacheAlien; }
  void ResetErrorCounters();
  void TearDown2ReadOnly();

  std::string cache_dir() { return cache_dir_; }
  CacheManager *cache_mgr() { return cache_mgr_; }
  int cache_mode() { return cache_mode_; }
  std::string exe_path() { return exe_path_; }
  bool found_previous_crash() { return found_previous_crash_; }
  perf::Counter *n_fs_dir_open() { return n_fs_dir_open_; }
  perf::Counter *n_fs_forget() { return n_fs_forget_; }
  perf::Counter *n_fs_lookup() { return n_fs_lookup_; }
  perf::Counter *n_fs_lookup_negative() { return n_fs_lookup_negative_; }
  perf::Counter *n_fs_open() { return n_fs_open_; }
  perf::Counter *n_fs_read() { return n_fs_read_; }
  perf::Counter *n_fs_readlink() { return n_fs_readlink_; }
  perf::Counter *n_fs_stat() { return n_fs_stat_; }
  perf::Counter *n_io_error() { return n_io_error_; }
  std::string nfs_maps_dir() { return nfs_maps_dir_; }
  perf::Counter *no_open_dirs() { return no_open_dirs_; }
  perf::Counter *no_open_files() { return no_open_files_; }
  OptionsManager *options_mgr() { return options_mgr_; }
  int64_t quota_limit() { return quota_limit_; }
  std::string second_cache_dir() { return second_cache_dir_; }
  int second_cache_mode() { return cache_mode_; }
  perf::Statistics *statistics() { return statistics_; }
  Type type() { return type_; }
  cvmfs::Uuid *uuid_cache() { return uuid_cache_; }
  std::string workspace() { return workspace_; }

 private:
  /**
   * Only one instance may be alive at any given time
   */
  static bool g_alive;
  static const char *kDefaultCacheBase;  // /var/lib/cvmfs
  static const unsigned kDefaultQuotaLimit = 1024 * 1024 * 1024;  // 1GB

  static void LogSqliteError(void *user_data __attribute__((unused)),
                             int sqlite_extended_error,
                             const char *message);

  explicit FileSystem(const FileSystemInfo &fs_info);

  void SetupLogging();
  void CreateStatistics();
  void SetupSqlite();
  bool SetupWorkspace();
  bool SetupCwd();
  bool LockWorkspace();
  bool SetupCrashGuard();
  bool CreateCache();
  bool SetupQuotaMgmt();
  bool SetupNfsMaps();
  void SetupUuid();

  bool CheckCacheMode();
  void DetermineCacheMode();
  void DetermineCacheDirs();
  void DetermineMountpoint();

  // See FileSystemInfo for the following fields
  std::string name_;
  std::string exe_path_;
  Type type_;
  /**
   * Not owned by the FileSystem object
   */
  OptionsManager *options_mgr_;
  bool wait_workspace_;
  bool foreground_;

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

  /**
   * A writeable local directory.  Only small amounts of data (few bytes) will
   * be stored here.  Needed because the cache can be read-only.  The workspace
   * and the cache directory can be identical.  A workspace can be shared among
   * FileSystem instances if their name is different.
   */
  std::string workspace_;
  int fd_workspace_lock_;
  std::string path_workspace_lock_;

  /**
   * An empty file that is removed on proper shutdown.
   */
  std::string path_crash_guard_;

  /**
   * A crash guard was found, thus we assume the file system was not shutdown
   * properly last time.
   */
  bool found_previous_crash_;

  /**
   * Only needed for fuse to detect and prevent double mounting at the same
   * location.
   */
  std::string mountpoint_;
  std::string cache_dir_;
  std::string second_cache_dir_;
  std::string nfs_maps_dir_;
  /**
   * Combination of kCache... flags
   */
  int cache_mode_;
  /**
   * The lower cache tier currently can only be
   */
  int second_cache_mode_;
  /**
   * Soft limit in bytes for the cache.  The quota manager removes half the
   * cache when the limit is exceeded.
   */
  int64_t quota_limit_;
  CacheManager *cache_mgr_;
  /**
   * Persistent for the cache directory + name combination.  It is used in the
   * Geo-API to allow for per-client responses when no proxy is used.
   */
  cvmfs::Uuid *uuid_cache_;

  /**
   * Used internally to remember if NFS maps need to be shut down.
   */
  bool has_nfs_maps_;
  /**
   * Used internally to remember if the Sqlite memory manager need to be shut
   * down.
   */
  bool has_custom_sqlitevfs_;

  /**
   * Indicates which type of cache to use
   */
  CacheManagerIds cache_mgr_type_;
};


/**
 * A MountPoint provides a clip around all the different *Manager objects that
 * in combination represent a mounted cvmfs repository.  Its main purpose is
 * the controlled construction and deconstruction of the involved ensemble of
 * classes based on the information passed from an options manager.
 *
 * A MountPoint is constructed on top of a successfully constructed FileSystem.
 *
 * We use pointers to manager classes to make the order of construction and
 * destruction explicit and also to keep the include list for this header small.
 */
class MountPoint : SingleCopy, public BootFactory {
 public:
  /**
   * If catalog reload fails, try again in 3 minutes
   */
  static const unsigned kShortTermTTL = 180;
  static const time_t kIndefiniteDeadline = time_t(-1);

  static MountPoint *Create(const std::string &fqrn,
                            FileSystem *file_system,
                            OptionsManager *options_mgr = NULL);
  ~MountPoint();

  unsigned GetMaxTtlMn();
  unsigned GetEffectiveTtlSec();
  void SetMaxTtlMn(unsigned value_minutes);
  void ReEvaluateAuthz();

  AuthzSessionManager *authz_session_mgr() { return authz_session_mgr_; }
  BackoffThrottle *backoff_throttle() { return backoff_throttle_; }
  catalog::ClientCatalogManager *catalog_mgr() { return catalog_mgr_; }
  ChunkTables *chunk_tables() { return chunk_tables_; }
  download::DownloadManager *download_mgr() { return download_mgr_; }
  download::DownloadManager *external_download_mgr() {
    return external_download_mgr_;
  }
  cvmfs::Fetcher *fetcher() { return fetcher_; }
  bool fixed_catalog() { return fixed_catalog_; }
  std::string fqrn() { return fqrn_; }
  cvmfs::Fetcher *external_fetcher() { return external_fetcher_; }
  FileSystem *file_system() { return file_system_; }
  bool has_membership_req() { return has_membership_req_; }
  bool hide_magic_xattrs() { return hide_magic_xattrs_; }
  catalog::InodeGenerationAnnotation *inode_annotation() {
    return inode_annotation_;
  }
  glue::InodeTracker *inode_tracker() { return inode_tracker_; }
  lru::InodeCache *inode_cache() { return inode_cache_; }
  double kcache_timeout_sec() { return kcache_timeout_sec_; }
  lru::Md5PathCache *md5path_cache() { return md5path_cache_; }
  std::string membership_req() { return membership_req_; }
  lru::PathCache *path_cache() { return path_cache_; }
  std::string repository_tag() { return repository_tag_; }
  SimpleChunkTables *simple_chunk_tables() { return simple_chunk_tables_; }
  perf::Statistics *statistics() { return statistics_; }
  signature::SignatureManager *signature_mgr() { return signature_mgr_; }
  Tracer *tracer() { return tracer_; }
  cvmfs::Uuid *uuid() { return uuid_; }

  /**
   * Used by hotpatch procedure
   */
  void set_inode_tracker(glue::InodeTracker *val) { inode_tracker_ = val; }
  void set_chunk_tables(ChunkTables *val) { chunk_tables_ = val; }

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
  /**
   * Memory buffer sizes for an activated tracer
   */
  static const unsigned kTracerBufferSize = 8192;
  static const unsigned kTracerFlushThreshold = 7000;
  static const char *kDefaultBlacklist;  // "/etc/cvmfs/blacklist"

  MountPoint(const std::string &fqrn,
             FileSystem *file_system,
             OptionsManager *options_mgr);

  void CreateStatistics();
  void CreateAuthz();
  bool CreateSignatureManager();
  bool CheckBlacklists();
  bool CreateDownloadManagers();
  void CreateFetchers();
  bool CreateCatalogManager();
  void CreateTables();
  bool CreateTracer();
  void SetupBehavior();
  void SetupDnsTuning(download::DownloadManager *manager);
  void SetupHttpTuning();
  bool SetupExternalDownloadMgr();
  void SetupInodeAnnotation();
  bool SetupOwnerMaps();
  bool DetermineRootHash(shash::Any *root_hash);
  bool FetchHistory(std::string *history_path);
  std::string ReplaceHosts(std::string hosts);

  std::string fqrn_;
  cvmfs::Uuid *uuid_;
  /**
   * In contrast to the manager objects, the FileSystem is not owned.
   */
  FileSystem *file_system_;
  /**
   * The options manager is not owned.
   */
  OptionsManager *options_mgr_;

  perf::Statistics *statistics_;
  AuthzFetcher *authz_fetcher_;
  AuthzSessionManager *authz_session_mgr_;
  AuthzAttachment *authz_attachment_;
  BackoffThrottle *backoff_throttle_;
  signature::SignatureManager *signature_mgr_;
  download::DownloadManager *download_mgr_;
  download::DownloadManager *external_download_mgr_;
  cvmfs::Fetcher *fetcher_;
  cvmfs::Fetcher *external_fetcher_;
  catalog::InodeGenerationAnnotation *inode_annotation_;
  catalog::ClientCatalogManager *catalog_mgr_;
  ChunkTables *chunk_tables_;
  SimpleChunkTables *simple_chunk_tables_;
  lru::InodeCache *inode_cache_;
  lru::PathCache *path_cache_;
  lru::Md5PathCache *md5path_cache_;
  Tracer *tracer_;
  glue::InodeTracker *inode_tracker_;

  unsigned max_ttl_sec_;
  pthread_mutex_t lock_max_ttl_;
  double kcache_timeout_sec_;
  bool fixed_catalog_;
  bool hide_magic_xattrs_;
  std::string repository_tag_;

  // TODO(jblomer): this should go in the catalog manager
  std::string membership_req_;
  bool has_membership_req_;
};  // class MointPoint

#endif  // CVMFS_MOUNTPOINT_H_
