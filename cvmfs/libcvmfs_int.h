/**
 * This file is part of the CernVM File System.
 *
 * This is the internal implementation of libcvmfs,
 * not to be exposed to the code using the library.
 */

#ifndef CVMFS_LIBCVMFS_INT_H_
#define CVMFS_LIBCVMFS_INT_H_

#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "backoff.h"
#include "catalog_mgr.h"
#include "file_chunk.h"
#include "lru.h"
#include "util.h"

namespace cache {
class CacheManager;
}

namespace catalog {
class ClientCatalogManager;
}

namespace signature {
class SignatureManager;
}

namespace download {
class DownloadManager;
}

namespace perf {
class Statistics;
}

namespace cvmfs {
extern pid_t         pid_;
extern std::string  *repository_name_;
extern bool          foreground_;
class Fetcher;
}

/**
 * A singleton managing the cvmfs resources for all attached repositories.
 */
class cvmfs_globals : SingleCopy {
 public:
  // Common options for all repositories
  struct options {
    options()
      : change_to_cache_directory(false)
      , alien_cache(false)
      , syslog_level(-1)
      , log_syslog_level(-1)
      , nofiles(-1)
      , max_open_files(0)
      , quota_limit(0)
      , quota_threshold(0)
      , rebuild_cachedb(0)
    { }

    std::string    cache_directory;
    std::string    cachedir;  // Alias of cache_directory
    std::string    alien_cachedir;
    std::string    lock_directory;
    bool           change_to_cache_directory;
    bool           alien_cache;

    int            syslog_level;
    int            log_syslog_level;
    std::string    log_prefix;
    std::string    logfile;
    std::string    log_file;

    int            nofiles;
    int            max_open_files;  // Alias of nofiles

    // Currently ignored
    unsigned quota_limit;
    unsigned quota_threshold;
    bool rebuild_cachedb;
  };

  static int            Initialize(const options &opts);
  static void           Destroy();
  static cvmfs_globals* Instance();

  pthread_mutex_t *libcrypto_locks() { return libcrypto_locks_; }
  cache::CacheManager *cache_mgr() { return cache_mgr_; }

 protected:
  int Setup(const options &opts);
  static void CallbackLibcryptoLock(int mode, int type,
                                    const char *file, int line);
  // unsigned long type required by libcrypto (openssl)
  static unsigned long CallbackLibcryptoThreadId();  // NOLINT

 private:
  cvmfs_globals();
  ~cvmfs_globals();

  static cvmfs_globals *instance;

  perf::Statistics *statistics_;
  cache::CacheManager *cache_mgr_;
  std::string       cache_directory_;
  std::string       lock_directory_;
  uid_t             uid_;
  gid_t             gid_;
  int               fd_lockfile_;
  pthread_mutex_t  *libcrypto_locks_;
  void             *sqlite_page_cache;
  bool lock_created_;
  bool vfs_registered_;
};


/**
 * Encapsulates state and manager objects for a single attached repository.
 */
class cvmfs_context : SingleCopy {
 public:
  struct options {
    unsigned       timeout;
    unsigned       timeout_direct;
    std::string    url;
    std::string    external_url;
    std::string    proxies;
    std::string    fallback_proxies;
    std::string    tracefile;  // unused
    std::string    pubkey;
    std::string    deep_mount;
    std::string    blacklist;
    std::string    repo_name;
    std::string    root_hash;
    std::string    mountpoint;
    bool           allow_unsigned;

   public:
    options() :
      timeout(2),
      timeout_direct(2),
      pubkey("/etc/cvmfs/keys/cern.ch.pub"),
      blacklist(""),
      allow_unsigned(false) {}
  };

  static cvmfs_context* Create(const options &options);
  static void Destroy(cvmfs_context *ctx);

  int GetAttr(const char *c_path, struct stat *info);
  int Readlink(const char *path, char *buf, size_t size);
  int ListDirectory(const char *path, char ***buf, size_t *buflen);

  int Open(const char *c_path);
  int64_t Pread(int fd, void *buf, uint64_t size, uint64_t off);
  int Close(int fd);

  catalog::LoadError RemountStart();

  perf::Statistics *statistics() const { return statistics_; }
  std::string mountpoint() const { return cfg_.mountpoint; }

 protected:
  /**
   * use static method Create() for construction
   */
  explicit cvmfs_context(const options &options);
  ~cvmfs_context();

 private:
  static const int kFdChunked = 1 << 30;

  int Setup(const options &opts, perf::Statistics *statistics);

  void InitRuntimeCounters();

  void AppendStringToList(char const   *str,
                          char       ***buf,
                          size_t       *listlen,
                          size_t       *buflen);

  bool GetDirentForPath(const PathString         &path,
                        catalog::DirectoryEntry  *dirent);

  perf::Statistics *statistics_;

  const options cfg_;

  std::string mountpoint_;
  std::string cachedir_;
  std::string tracefile_;
  /**
   * Expected repository name, e.g. atlas.cern.ch
   */
  std::string repository_name_;
  pid_t pid_;  /**< will be set after deamon() */
  time_t boot_time_;
  catalog::ClientCatalogManager *catalog_manager_;
  signature::SignatureManager *signature_manager_;
  download::DownloadManager *download_manager_;
  download::DownloadManager *external_download_manager_;
  cvmfs::Fetcher *fetcher_;
  cvmfs::Fetcher *external_fetcher_;
  lru::Md5PathCache *md5path_cache_;

  atomic_int64 num_fs_open_;
  atomic_int64 num_fs_dir_open_;
  atomic_int64 num_fs_lookup_;
  atomic_int64 num_fs_lookup_negative_;
  atomic_int64 num_fs_stat_;
  atomic_int64 num_fs_read_;
  atomic_int64 num_fs_readlink_;
  atomic_int32 num_io_error_;
  atomic_int32 open_dirs_; /**< number of currently open directories */
  /**
   * Number of reserved file descriptors for internal use
   */
  static const int kNumReservedFd = 512;
  static const unsigned int kMd5pathCacheSize = 32000;

  BackoffThrottle backoff_throttle_;
  SimpleChunkTables chunk_tables_;

  int fd_lockfile;

  bool download_ready_;
  bool external_download_ready_;
  bool signature_ready_;
  bool catalog_ready_;
  bool pathcache_ready_;
};

#endif  // CVMFS_LIBCVMFS_INT_H_
