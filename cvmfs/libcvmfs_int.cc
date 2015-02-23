/**
 * This file is part of the CernVM File System.
 *
 * CernVM-FS is a FUSE module which implements an HTTP read-only filesystem.
 * The original idea is based on GROW-FS.
 *
 * CernVM-FS shows a remote HTTP directory as local file system.  The client
 * sees all available files.  On first access, a file is downloaded and
 * cached locally.  All downloaded pieces are verified by a cryptographic hash.
 *
 * To do so, a directory hive has to be transformed into a CVMFS2
 * "repository".  This can be done by the CernVM-FS server tools.
 *
 * This preparation of directories is transparent to web servers and
 * web proxies.  They just serve static content, i.e. arbitrary files.
 * Any HTTP server should do the job.  We use Apache + Squid.  Serving
 * files from the memory of a web proxy brings a significant performance
 * improvement.
 *
 * This is the internal implementation of libcvmfs, not to be exposed
 * to the code using the library.  This code is based heavily on the
 * fuse module cvmfs.cc.
 */

#define ENOATTR ENODATA  /**< instead of including attr/xattr.h */

#include <sys/xattr.h>
#include "cvmfs_config.h"
#include "libcvmfs_int.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <google/dense_hash_map>
#include <openssl/crypto.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/stat.h>
#ifndef __APPLE__
#include <sys/statfs.h>
#endif
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "backoff.h"
#include "cache.h"
#include "compression.h"
#include "directory_entry.h"
#include "download.h"
#include "duplex_sqlite3.h"
#include "globals.h"
#include "hash.h"
#include "logging.h"
#include "lru.h"
#include "monitor.h"
#include "murmur.h"
#include "platform.h"
#include "quota.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "tracer.h"
#include "util.h"
#include "wpad.h"

using namespace std;  // NOLINT

namespace cvmfs {
  pid_t    pid_             = 0;
  string  *repository_name_ = new string("=multipe=");
  bool     foreground_      = false;
}


cvmfs_globals* cvmfs_globals::instance = NULL;
cvmfs_globals* cvmfs_globals::Instance() {
  assert(cvmfs_globals::instance != NULL);
  return cvmfs_globals::instance;
}

int cvmfs_globals::Initialize(const options &opts) {
  assert(cvmfs_globals::instance == NULL);

  // create singleton instance
  cvmfs_globals::instance = new cvmfs_globals;
  assert(cvmfs_globals::instance != NULL);

  // setup the globals
  const int retval = cvmfs_globals::instance->Setup(opts);
  if (retval != 0) {
    delete cvmfs_globals::instance;
    cvmfs_globals::instance = NULL;
  }

  return retval;
}

void cvmfs_globals::Destroy() {
  if (cvmfs_globals::instance != NULL) {
    delete cvmfs_globals::instance;
    cvmfs_globals::instance = NULL;
  }
  assert(cvmfs_globals::instance == NULL);
}

cvmfs_globals::cvmfs_globals() :
  sqlite_scratch(NULL),
  sqlite_page_cache(NULL),
  options_ready_(false),
  lock_created_(false),
  cache_ready_(false),
  quota_ready_(false) {}

cvmfs_globals::~cvmfs_globals() {
  if (lock_created_) {
    UnlockFile(fd_lockfile_);
  }

  if (cache_ready_) {
    cache::Fini();
  }

  if (quota_ready_) {
    quota::Fini();
  }
}

int cvmfs_globals::Setup(const options &opts) {
  // Fill cvmfs option variables from arguments
  cache_directory_ = opts.cache_directory;
  uid_ = getuid();
  gid_ = getgid();
  options_ready_ = true;

  int retval;

  // Tune SQlite3 memory
  sqlite_scratch = smalloc(8192*16);  // 8 KB for 8 threads (2 slots per thread)
  sqlite_page_cache = smalloc(1280*3275);  // 4MB
  retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, sqlite_scratch, 8192, 16);
  assert(retval == SQLITE_OK);
  retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, sqlite_page_cache,
                          1280, 3275);
  assert(retval == SQLITE_OK);
  // 4 KB
  retval = sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 32, 128);
  assert(retval == SQLITE_OK);

  // Libcrypto
  libcrypto_locks_ = static_cast<pthread_mutex_t *>(OPENSSL_malloc(
                      CRYPTO_num_locks() * sizeof(pthread_mutex_t)));
  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    int retval = pthread_mutex_init(&(libcrypto_locks_[i]), NULL);
    assert(retval == 0);
  }
  CRYPTO_set_id_callback(cvmfs_globals::CallbackLibcryptoThreadId);
  CRYPTO_set_locking_callback(cvmfs_globals::CallbackLibcryptoLock);

  // Logging
  SetLogSyslogLevel(opts.log_syslog_level);
  if (!opts.log_prefix.empty()) {
    SetLogSyslogPrefix(opts.log_prefix);
  } else {
    SetLogSyslogPrefix("libcvmfs");
  }
  if (!opts.log_file.empty()) {
    SetLogDebugFile(opts.log_file);
  }

  // Maximum number of open files
  if (opts.max_open_files > 0) {
    struct rlimit rpl;
    memset(&rpl, 0, sizeof(rpl));
    getrlimit(RLIMIT_NOFILE, &rpl);
    if (rpl.rlim_max < (unsigned)opts.max_open_files)
      rpl.rlim_max = opts.max_open_files;
    rpl.rlim_cur = opts.max_open_files;
    if (setrlimit(RLIMIT_NOFILE, &rpl) != 0) {
      PrintError("Failed to set maximum number of open files, "
                 "insufficient permissions");
      return -1;
    }
  }

  // Create cache directory, if necessary
  if (!MkdirDeep(cache_directory_, 0700)) {
    PrintError("cannot create cache directory " + cache_directory_);
    return -2;
  }

  // Try to jump to cache directory.  This tests, if it is accassible.
  // Also, it brings speed later on.
  if (opts.change_to_cache_directory &&
      chdir(cache_directory_.c_str()) != 0) {
    PrintError("cache directory " + cache_directory_ + " is unavailable");
    return -3;
  }

  // Create lock file and running sentinel
  fd_lockfile_ = LockFile(cache_directory_ + "/lock.libcvmfs");
  if (fd_lockfile_ < 0) {
    PrintError("could not acquire lock (" + StringifyInt(errno) + ")");
    return -4;
  }
  lock_created_ = true;

  // Creates a set of cache directories (256 directories named 00..ff) if not
  // using alien cachdir
  if (!cache::Init(cache_directory_, opts.alien_cache)) {
    PrintError("Failed to setup cache in " + cache_directory_ +
               ": " + strerror(errno));
    return -5;
  }
  cache_ready_ = true;

  // Init quota / managed cache
  LogCvmfs(kLogCvmfs, kLogDebug, "unlimited cache size");
  const uint64_t quota_limit      = (uint64_t) -1;
  const uint64_t quota_threshold  = 0;
  const bool     rebuild_database = false;
  if (!quota::Init(cache_directory_, quota_limit, quota_threshold,
                   rebuild_database))
  {
    PrintError("Failed to initialize lru cache");
    return -6;
  }
  quota::Spawn();
  quota_ready_ = true;

  cvmfs::pid_ = getpid();

  return 0;
}

void cvmfs_globals::CallbackLibcryptoLock(int mode, int type,
                                          const char *file, int line) {
  (void)file;
  (void)line;

  int retval;
  cvmfs_globals   *globals = cvmfs_globals::Instance();
  pthread_mutex_t *locks   = globals->libcrypto_locks();
  pthread_mutex_t *lock    = &(locks[type]);

  if (mode & CRYPTO_LOCK) {
    retval = pthread_mutex_lock(lock);
  } else {
    retval = pthread_mutex_unlock(lock);
  }

  assert(retval == 0);
}

// Type unsigned long required by libcrypto (openssl)
unsigned long cvmfs_globals::CallbackLibcryptoThreadId() {  // NOLINT
  return platform_gettid();
}


cvmfs_context* cvmfs_context::Create(const options &opts) {
  cvmfs_context *ctx = new cvmfs_context(opts);
  assert(ctx != NULL);

  if (ctx->Setup(opts) != 0) {
    delete ctx;
    ctx = NULL;
  }

  return ctx;
}

void cvmfs_context::Destroy(cvmfs_context *ctx) {
  delete ctx;
}

int cvmfs_context::Setup(const options &opts) {
  // Network initialization
  download_manager_ = new download::DownloadManager();
  download_manager_->Init(16, false);
  download_manager_->SetHostChain(opts.url);
  download_manager_->SetTimeout(opts.timeout,
                                opts.timeout_direct);
  download_manager_->SetProxyChain(
    download::ResolveProxyDescription(opts.proxies, download_manager_),
    opts.fallback_proxies,
    download::DownloadManager::kSetProxyBoth);
  // ctx.download_manager_->EnableInfoHeader();
  download_ready_ = true;

  signature_manager_ = new signature::SignatureManager();
  signature_manager_->Init();
  if (!signature_manager_->LoadPublicRsaKeys(opts.pubkey)) {
    PrintError("failed to load public key(s)");
    return -1;
  } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: using public key(s) %s",
               JoinStrings(
                 SplitString(opts.pubkey, ':'), ", ").c_str());
  }
  signature_ready_ = true;

  if (!opts.blacklist.empty()) {
    if (!signature_manager_->LoadBlacklist(opts.blacklist)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "failed to load blacklist");
      return -2;
    }
  }

  // Load initial file catalog
  catalog_manager_ = new cache::CatalogManager(repository_name_,
                                               signature_manager_,
                                               download_manager_);
  bool clg_mgr_init;
  if (!opts.root_hash.empty()) {
    const shash::Any hash = shash::MkFromHexPtr(shash::HexPtr(opts.root_hash),
                                                shash::kSuffixCatalog);
    clg_mgr_init = catalog_manager_->InitFixed(hash);
  } else {
    clg_mgr_init = catalog_manager_->Init();
  }
  if (!clg_mgr_init) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to initialize root file catalog");
    return -1;
  }
  catalog_ready_ = true;

  // Set fuse callbacks, remove url from arguments
  LogCvmfs(kLogCvmfs, kLogSyslog,
           "CernVM-FS: linking %s to repository %s",
           opts.mountpoint.c_str(), repository_name_.c_str());

  md5path_cache_ = new lru::Md5PathCache(cvmfs_context::kMd5pathCacheSize);
  pathcache_ready_ = true;

  if (!opts.tracefile.empty()) {
    tracer::Init(8192, 7000, opts.tracefile);
  } else {
    tracer::InitNull();
  }
  tracer_ready_ = true;

  return 0;
}

cvmfs_context::cvmfs_context(const options &opts) :
  cfg_(opts),
  repository_name_(opts.repo_name),
  boot_time_(time(NULL)),
  md5path_cache_(NULL),
  backoff_throttle_(new BackoffThrottle),
  fd_lockfile(-1),
  download_ready_(false),
  signature_ready_(false),
  catalog_ready_(false),
  pathcache_ready_(false),
  tracer_ready_(false)
{
  InitRuntimeCounters();
}

cvmfs_context::~cvmfs_context() {
  if (download_ready_) {
    download_manager_->Fini();
    delete download_manager_;
    download_manager_ = NULL;
  }

  if (signature_ready_) {
    signature_manager_->Fini();
    delete signature_manager_;
    signature_manager_ = NULL;
  }

  if (catalog_ready_) {
    delete catalog_manager_;
    catalog_manager_ = NULL;
  }

  if (pathcache_ready_) {
    delete md5path_cache_;
    md5path_cache_ = NULL;
  }

  if (tracer_ready_) {
    tracer::Fini();
  }

  delete backoff_throttle_;
  backoff_throttle_ = NULL;
}

bool cvmfs_context::GetDirentForPath(const PathString         &path,
                                     catalog::DirectoryEntry  *dirent)
{
  if (path.GetLength() == 1 && path.GetChars()[0] == '/') {
    // root path is expected to be "", not "/"
    PathString p;
    return GetDirentForPath(p, dirent);
  }
  shash::Md5 md5path(path.GetChars(), path.GetLength());
  if (md5path_cache_->Lookup(md5path, dirent))
    return dirent->GetSpecial() != catalog::kDirentNegative;

  // Lookup inode in catalog TODO: not twice md5 calculation
  if (catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent)) {
    md5path_cache_->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  // Only cache real ENOENT errors, not catalog load errors
  if (dirent->GetSpecial() == catalog::kDirentNegative)
    md5path_cache_->InsertNegative(md5path);

  return false;
}

void cvmfs_context::AppendStringToList(char const   *str,
                                       char       ***buf,
                                       size_t       *listlen,
                                       size_t       *buflen)
{
  if (*listlen + 1 >= *buflen) {
       size_t newbuflen = (*listlen)*2 + 5;
       *buf = reinterpret_cast<char **>(
         realloc(*buf, sizeof(char *) * newbuflen));
       assert(*buf);
       *buflen = newbuflen;
       assert(*listlen < *buflen);
  }
  if (str) {
    (*buf)[(*listlen)] = strdup(str);
    // null-terminate the list
    (*buf)[++(*listlen)] = NULL;
  } else {
    (*buf)[(*listlen)] = NULL;
  }
}


int cvmfs_context::GetAttr(const char *c_path, struct stat *info) {
  atomic_inc64(&num_fs_stat_);

  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_getattr (stat) for path: %s", c_path);

  PathString p;
  p.Assign(c_path, strlen(c_path));

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForPath(p, &dirent);

  if (!found) {
    return -ENOENT;
  }

  *info = dirent.GetStatStructure();
  return 0;
}

int cvmfs_context::Readlink(const char *c_path, char *buf, size_t size) {
  atomic_inc64(&num_fs_readlink_);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_readlink on path: %s", c_path);

  PathString p;
  p.Assign(c_path, strlen(c_path));

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForPath(p, &dirent);

  if (!found) {
    return -ENOENT;
  }

  if (!dirent.IsLink()) {
    return -EINVAL;
  }

  unsigned len = (dirent.symlink().GetLength() >= size) ?
    size : dirent.symlink().GetLength() + 1;
  strncpy(buf, dirent.symlink().c_str(), len-1);
  buf[len-1] = '\0';

  return 0;
}

int cvmfs_context::ListDirectory(
  const char *c_path,
  char ***buf,
  size_t *buflen
) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_listdir on path: %s", c_path);

  if (c_path[0] == '/' && c_path[1] == '\0') {
    // root path is expected to be "", not "/"
    c_path = "";
  }

  PathString path;
  path.Assign(c_path, strlen(c_path));

  catalog::DirectoryEntry d;
  const bool found = GetDirentForPath(path, &d);

  if (!found) {
    return -ENOENT;
  }

  if (!d.IsDirectory()) {
    return -ENOTDIR;
  }

  size_t listlen = 0;
  AppendStringToList(NULL, buf, &listlen, buflen);

  // Build listing

  // Add current directory link
  AppendStringToList(".", buf, &listlen, buflen);

  // Add parent directory link
  catalog::DirectoryEntry p;
  if (d.inode() != catalog_manager_->GetRootInode()) {
    AppendStringToList("..", buf, &listlen, buflen);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  if (!catalog_manager_->ListingStat(path, &listing_from_catalog)) {
    return -EIO;
  }
  for (unsigned i = 0; i < listing_from_catalog.size(); ++i) {
    AppendStringToList(listing_from_catalog.AtPtr(i)->name.c_str(),
                          buf, &listlen, buflen);
  }

  return 0;
}

int cvmfs_context::Open(const char *c_path) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on path: %s", c_path);

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;
  path.Assign(c_path, strlen(c_path));

  const bool found = GetDirentForPath(path, &dirent);

  if (!found) {
    return -ENOENT;
  }

  const bool volatile_content = false;
  fd = cache::FetchDirent(dirent, string(path.GetChars(), path.GetLength()),
                          volatile_content, download_manager_);
  atomic_inc64(&num_fs_open_);

  if (fd >= 0) {
    if ((atomic_xadd32(&open_files_, 1) <
        (static_cast<int>(max_open_files_)) - kNumReservedFd) ||
        max_open_files_ == 0)
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
      return fd;
    } else {
      if (close(fd) == 0) atomic_dec32(&open_files_);
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "open file descriptor limit exceeded");
      return -EMFILE;
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to open path: %s, CAS key %s, error code %d",
             c_path, dirent.checksum().ToString().c_str(), errno);
    if (errno == EMFILE) {
      return -EMFILE;
    }
  }

  // Prevent Squid DoS
  backoff_throttle_->Throttle();

  atomic_inc32(&num_io_error_);
  return fd;
}

int cvmfs_context::Close(int fd) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_close on file number: %d", fd);

  if (close(fd) == 0) atomic_dec32(&open_files_);

  return 0;
}

catalog::LoadError cvmfs_context::RemountStart() {
  return catalog::kLoadNew;
}

void cvmfs_context::InitRuntimeCounters() {
  // Runtime counters
  atomic_init64(&num_fs_open_);
  atomic_init64(&num_fs_dir_open_);
  atomic_init64(&num_fs_lookup_);
  atomic_init64(&num_fs_lookup_negative_);
  atomic_init64(&num_fs_stat_);
  atomic_init64(&num_fs_read_);
  atomic_init64(&num_fs_readlink_);
  atomic_init32(&num_io_error_);

  atomic_init32(&open_files_);
  atomic_init32(&open_dirs_);
}
