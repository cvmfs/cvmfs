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
#include <syslog.h>

#include <string>
#include <vector>

#include "catalog_mgr.h"
#include "lru.h"
#include "util.h"

namespace cache {
  class CatalogManager;
}

namespace signature {
  class SignatureManager;
}

namespace download {
  class DownloadManager;
}

class BackoffThrottle;

namespace cvmfs {

  extern pid_t         pid_;
  extern std::string  *repository_name_;
  extern bool          foreground_;

}

class cvmfs_globals : SingleCopy {
 public:
  struct options {
    options() : change_to_cache_directory(false),
                alien_cache(false),
                log_syslog_level(LOG_ALERT),
                max_open_files(0) {}

    std::string    cache_directory;
    bool           change_to_cache_directory;
    bool           alien_cache;

    int            log_syslog_level;
    std::string    log_prefix;
    std::string    log_file;

    int            max_open_files;
  };

 public:
  static int            Initialize(const options &opts);
  static void           Destroy();
  static cvmfs_globals* Instance();

  pthread_mutex_t *libcrypto_locks() { return libcrypto_locks_; }

 protected:
  int Setup(const options &opts);

  static void CallbackLibcryptoLock(int mode, int type,
                                    const char *file, int line);
  static unsigned long CallbackLibcryptoThreadId();

 private:
  cvmfs_globals();
  ~cvmfs_globals();

 private:
  static cvmfs_globals *instance;

 private:
  std::string       cache_directory_;
  uid_t             uid_;
  gid_t             gid_;

  int               fd_lockfile_;
  pthread_mutex_t  *libcrypto_locks_;

  void             *sqlite_scratch;
  void             *sqlite_page_cache;

 private:
  bool options_ready_;
  bool lock_created_;
  bool cache_ready_;
  bool quota_ready_;
};

class cvmfs_context : SingleCopy {
 public:
  struct options {
    unsigned       timeout;
    unsigned       timeout_direct;
    std::string    url;
    std::string    proxies;
    std::string    fallback_proxies;
    std::string    tracefile;
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


 public:
  static cvmfs_context* Create(const options &options);
  static void Destroy(cvmfs_context *ctx);

  int GetAttr(const char *c_path, struct stat *info);
  int Readlink(const char *path, char *buf, size_t size);
  int ListDirectory(const char *path, char ***buf, size_t *buflen);

  int Open(const char *c_path);
  int Close(int fd);

  catalog::LoadError RemountStart();

 protected:
  cvmfs_context(const options &options); // please use static method Create()
                                         // for construction
  ~cvmfs_context();

 private:
  int Setup(const options &opts);

  void InitRuntimeCounters();

  void AppendStringToList(char const   *str,
                          char       ***buf,
                          size_t       *listlen,
                          size_t       *buflen);

  bool GetDirentForPath(const PathString         &path,
                        catalog::DirectoryEntry  *dirent);

 private:
  const options cfg_;

  std::string mountpoint_;
  std::string cachedir_;
  std::string relative_cachedir; /* path to cachedir, relative to current working dir */
  std::string tracefile_;
  std::string repository_name_;  /**< Expected repository name,
                                         e.g. atlas.cern.ch */
  pid_t pid_;  /**< will be set after deamon() */
  time_t boot_time_;
  cache::CatalogManager *catalog_manager_;
  signature::SignatureManager *signature_manager_;
  download::DownloadManager *download_manager_;
  lru::Md5PathCache *md5path_cache_;

  atomic_int64 num_fs_open_;
  atomic_int64 num_fs_dir_open_;
  atomic_int64 num_fs_lookup_;
  atomic_int64 num_fs_lookup_negative_;
  atomic_int64 num_fs_stat_;
  atomic_int64 num_fs_read_;
  atomic_int64 num_fs_readlink_;
  atomic_int32 num_io_error_;
  atomic_int32 open_files_; /**< number of currently open files by Fuse calls */
  atomic_int32 open_dirs_; /**< number of currently open directories */
  unsigned max_open_files_; /**< maximum allowed number of open files */
  static const int kNumReservedFd = 512;  /**< Number of reserved file
                                               descriptors for internal use */
  static const unsigned int kMd5pathCacheSize = 32000;

  BackoffThrottle *backoff_throttle_;

  int fd_lockfile;

 private:
  bool download_ready_;
  bool signature_ready_;
  bool catalog_ready_;
  bool pathcache_ready_;
  bool tracer_ready_;
};

#endif  // CVMFS_LIBCVMFS_INT_H_
