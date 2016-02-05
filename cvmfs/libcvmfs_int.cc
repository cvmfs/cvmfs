/**
 * This file is part of the CernVM File System.
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
#include "cache.h"
#include "catalog_mgr_client.h"
#include "clientctx.h"
#include "compression.h"
#include "directory_entry.h"
#include "download.h"
#include "duplex_sqlite3.h"
#include "fetch.h"
#include "globals.h"
#include "hash.h"
#include "libcvmfs.h"
#include "logging.h"
#include "lru.h"
#include "monitor.h"
#include "murmur.h"
#include "platform.h"
#include "quota.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "sqlitevfs.h"
#include "statistics.h"
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

cvmfs_globals::cvmfs_globals()
  : statistics_(new perf::Statistics())
  , cache_mgr_(NULL)
  , uid_(0)
  , gid_(0)
  , fd_lockfile_(-1)
  , libcrypto_locks_(NULL)
  , sqlite_page_cache(NULL)
  , lock_created_(false)
  , vfs_registered_(false)
  { }

cvmfs_globals::~cvmfs_globals() {
  if (vfs_registered_)
    sqlite::UnregisterVfsRdOnly();

  delete cache_mgr_;
  cache_mgr_ = NULL;

  if (lock_created_) {
    UnlockFile(fd_lockfile_);
  }

  if (libcrypto_locks_) {
    CRYPTO_set_locking_callback(NULL);
    for (int i = 0; i < CRYPTO_num_locks(); ++i)
      pthread_mutex_destroy(&(libcrypto_locks_[i]));
    OPENSSL_free(libcrypto_locks_);
  }

  sqlite3_shutdown();
  delete statistics_;

  ClientCtx::CleanupInstance();
}

int cvmfs_globals::Setup(const options &opts) {
  // Fill cvmfs option variables from arguments
  cache_directory_ = opts.cache_directory;
  lock_directory_ = opts.lock_directory;
  if (!lock_directory_.size()) {
    lock_directory_ = cache_directory_;
  }
  uid_ = getuid();
  gid_ = getgid();

  int retval;

  // Tune SQlite3
  sqlite_page_cache = smalloc(1280*3275);  // 4MB
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
      return LIBCVMFS_FAIL_NOFILES;
    }
  }

  // Create cache directory, if necessary
  if (!MkdirDeep(cache_directory_, 0700, false)) {
    PrintError("cannot create cache directory " + cache_directory_);
    return LIBCVMFS_FAIL_MKCACHE;
  }

  // Create lock directory, if necessary
  if (!MkdirDeep(lock_directory_, 0700)) {
    PrintError("cannot create lock directory " + lock_directory_);
    return LIBCVMFS_FAIL_MKCACHE;
  }

  // Create lock file protecting non-alien cache from concurrent access
  fd_lockfile_ = LockFile(lock_directory_ + "/lock.libcvmfs");
  if (fd_lockfile_ < 0) {
    PrintError("could not acquire lock (" + StringifyInt(errno) + ")");
    return LIBCVMFS_FAIL_LOCKFILE;
  }
  lock_created_ = true;

  if (opts.alien_cachedir != "") {
    cache_directory_ = opts.alien_cachedir;
  }
  // Try to jump to cache directory.  This tests, if it is accessible.
  // Also, it brings speed later on.
  if (opts.change_to_cache_directory) {
    if (opts.alien_cachedir != "")
      MkdirDeep(opts.alien_cachedir, 0770, false);
    if (chdir(cache_directory_.c_str()) != 0) {
      PrintError("cache directory " + cache_directory_ + " is unavailable");
      return LIBCVMFS_FAIL_OPENCACHE;
    }
  }
  // Creates a set of cache directories (256 directories named 00..ff) if not
  // using alien cachdir
  cache_mgr_ = cache::PosixCacheManager::Create(
    cache_directory_, opts.alien_cache || opts.alien_cachedir != "");
  if (cache_mgr_ == NULL) {
    PrintError("Failed to setup cache in " + cache_directory_ +
               ": " + strerror(errno));
    return LIBCVMFS_FAIL_INITCACHE;
  }

  retval = sqlite::RegisterVfsRdOnly(
    cache_mgr_, statistics_, sqlite::kVfsOptDefault);
  assert(retval);
  vfs_registered_ = true;

  cvmfs::pid_ = getpid();

  ClientCtx::GetInstance();

  return LIBCVMFS_FAIL_OK;
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

  perf::Statistics *statistics = new perf::Statistics();
  if (ctx->Setup(opts, statistics) != 0) {
    delete ctx;
    ctx = NULL;
  }

  return ctx;
}

void cvmfs_context::Destroy(cvmfs_context *ctx) {
  perf::Statistics *statistics = ctx->statistics();
  delete ctx;
  delete statistics;
}

int cvmfs_context::Setup(const options &opts, perf::Statistics *statistics) {
  statistics_ = statistics;

  // Network initialization
  download_manager_ = new download::DownloadManager();
  download_manager_->Init(16, false, statistics);
  download_manager_->SetHostChain(opts.url);
  download_manager_->SetTimeout(opts.timeout,
                                opts.timeout_direct);
  download_manager_->SetProxyChain(
    download::ResolveProxyDescription(opts.proxies, download_manager_),
    opts.fallback_proxies,
    download::DownloadManager::kSetProxyBoth);
  // ctx.download_manager_->EnableInfoHeader();
  download_ready_ = true;

  external_download_manager_ = new download::DownloadManager();
  external_download_manager_->Init(16, false, statistics, "download-external");
  external_download_manager_->SetHostChain(opts.external_url);
  external_download_manager_->SetTimeout(opts.timeout,
                                opts.timeout_direct);
  external_download_manager_->SetProxyChain(
    download::ResolveProxyDescription(opts.proxies, external_download_manager_),
    opts.fallback_proxies,
    download::DownloadManager::kSetProxyBoth);
  external_download_ready_ = true;

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
    const bool append = false;
    if (!signature_manager_->LoadBlacklist(opts.blacklist, append)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "failed to load blacklist");
      return -2;
    }
  }

  fetcher_ = new cvmfs::Fetcher(
    cvmfs_globals::Instance()->cache_mgr(),
    download_manager_,
    &backoff_throttle_,
    statistics_);

  external_fetcher_ = new cvmfs::Fetcher(
    cvmfs_globals::Instance()->cache_mgr(),
    external_download_manager_,
    &backoff_throttle_,
    statistics_,
    "fetch-external",
    true);

  // Load initial file catalog
  catalog_manager_ = new catalog::ClientCatalogManager(
    repository_name_, fetcher_, signature_manager_, statistics_);
  bool clg_mgr_init;
  if (!opts.root_hash.empty()) {
    const shash::Any hash = shash::MkFromHexPtr(shash::HexPtr(opts.root_hash),
                                                shash::kSuffixCatalog);
    clg_mgr_init = catalog_manager_->InitFixed(hash, false);
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

  md5path_cache_ = new lru::Md5PathCache(cvmfs_context::kMd5pathCacheSize,
      statistics);
  pathcache_ready_ = true;

  return 0;
}

cvmfs_context::cvmfs_context(const options &opts)
  : statistics_(NULL)
  , cfg_(opts)
  , repository_name_(opts.repo_name)
  , pid_(0)
  , boot_time_(time(NULL))
  , catalog_manager_(NULL)
  , signature_manager_(NULL)
  , download_manager_(NULL)
  , external_download_manager_(NULL)
  , fetcher_(NULL)
  , external_fetcher_(NULL)
  , md5path_cache_(NULL)
  , fd_lockfile(-1)
  , download_ready_(false)
  , external_download_ready_(false)
  , signature_ready_(false)
  , catalog_ready_(false)
  , pathcache_ready_(false)
{
  InitRuntimeCounters();
}

cvmfs_context::~cvmfs_context() {
  delete fetcher_;
  fetcher_ = NULL;
  delete external_fetcher_;
  external_fetcher_ = NULL;

  if (catalog_ready_) {
    delete catalog_manager_;
    catalog_manager_ = NULL;
  }

  if (download_ready_) {
    download_manager_->Fini();
    delete download_manager_;
    download_manager_ = NULL;
  }

  if (external_download_ready_) {
    external_download_manager_->Fini();
    delete external_download_manager_;
    external_download_manager_ = NULL;
  }

  if (signature_ready_) {
    signature_manager_->Fini();
    delete signature_manager_;
    signature_manager_ = NULL;
  }

  if (pathcache_ready_) {
    delete md5path_cache_;
    md5path_cache_ = NULL;
  }
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
  ClientCtxGuard ctxg(geteuid(), getegid(), getpid());

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
  ClientCtxGuard ctxg(geteuid(), getegid(), getpid());

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
  ClientCtxGuard ctxg(geteuid(), getegid(), getpid());

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
  ClientCtxGuard ctxg(geteuid(), getegid(), getpid());

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;
  path.Assign(c_path, strlen(c_path));

  const bool found = GetDirentForPath(path, &dirent);

  if (!found) {
    return -ENOENT;
  }

  if (dirent.IsChunkedFile()) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "chunked file %s opened (download delayed to read() call)",
             path.c_str());

    FileChunkList *chunks = new FileChunkList();
    if (!catalog_manager_->ListFileChunks(path, dirent.hash_algorithm(),
                                          chunks) ||
        chunks->IsEmpty())
    {
      LogCvmfs(kLogCvmfs, kLogDebug| kLogSyslogErr, "file %s is marked as "
               "'chunked', but no chunks found.", path.c_str());
      atomic_inc32(&num_io_error_);
      return -EIO;
    }

    fd = chunk_tables_.Add(
      FileChunkReflist(chunks, path, dirent.compression_algorithm(),
                       dirent.IsExternalFile()));
    return fd | kFdChunked;
  }

  fd = (dirent.IsExternalFile() ? external_fetcher_ : fetcher_)->Fetch(
    dirent.checksum(),
    dirent.size(),
    string(path.GetChars(), path.GetLength()),
    dirent.compression_algorithm(),
    cache::CacheManager::kTypeRegular);
  atomic_inc64(&num_fs_open_);

  if (fd >= 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
             path.c_str(), fd);
    return fd;
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to open path: %s, CAS key %s, error code %d",
             c_path, dirent.checksum().ToString().c_str(), errno);
    if (errno == EMFILE) {
      return -EMFILE;
    }
  }

  atomic_inc32(&num_io_error_);
  return fd;
}

int64_t cvmfs_context::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t off)
{
  if (fd & kFdChunked) {
    ClientCtxGuard ctxg(geteuid(), getegid(), getpid());
    const int chunk_handle = fd & ~kFdChunked;
    SimpleChunkTables::OpenChunks open_chunks = chunk_tables_.Get(chunk_handle);
    FileChunkList *chunk_list = open_chunks.chunk_reflist.list;
    zlib::Algorithms compression_alg =
      open_chunks.chunk_reflist.compression_alg;
    if (chunk_list == NULL)
      return -EBADF;

    // Fetch all needed chunks and read the requested data
    unsigned chunk_idx = open_chunks.chunk_reflist.FindChunkIdx(off);
    uint64_t overall_bytes_fetched = 0;
    off_t offset_in_chunk = off - chunk_list->AtPtr(chunk_idx)->offset();
    do {
      // Open file descriptor to chunk
      ChunkFd *chunk_fd = open_chunks.chunk_fd;
      if ((chunk_fd->fd == -1) || (chunk_fd->chunk_idx != chunk_idx)) {
        if (chunk_fd->fd != -1) fetcher_->cache_mgr()->Close(chunk_fd->fd);
        if (open_chunks.chunk_reflist.external_data) {
          chunk_fd->fd = external_fetcher_->Fetch(
            chunk_list->AtPtr(chunk_idx)->content_hash(),
            chunk_list->AtPtr(chunk_idx)->size(),
            "no path info",
            compression_alg,
            cache::CacheManager::kTypeRegular,
            open_chunks.chunk_reflist.path.ToString(),
            chunk_list->AtPtr(chunk_idx)->offset());
        } else {
          chunk_fd->fd = fetcher_->Fetch(
            chunk_list->AtPtr(chunk_idx)->content_hash(),
            chunk_list->AtPtr(chunk_idx)->size(),
            "no path info",
            compression_alg,
            cache::CacheManager::kTypeRegular);
        }
        if (chunk_fd->fd < 0) {
          chunk_fd->fd = -1;
          return -EIO;
        }
        chunk_fd->chunk_idx = chunk_idx;
      }

      LogCvmfs(kLogCvmfs, kLogDebug, "reading from chunk fd %d",
               chunk_fd->fd);
      // Read data from chunk
      const size_t bytes_to_read = size - overall_bytes_fetched;
      const size_t remaining_bytes_in_chunk =
        chunk_list->AtPtr(chunk_idx)->size() - offset_in_chunk;
      size_t bytes_to_read_in_chunk =
        std::min(bytes_to_read, remaining_bytes_in_chunk);
      const int64_t bytes_fetched = fetcher_->cache_mgr()->Pread(
        chunk_fd->fd,
        reinterpret_cast<char *>(buf) + overall_bytes_fetched,
        bytes_to_read_in_chunk,
        offset_in_chunk);

      if (bytes_fetched < 0) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr, "read err no %d (%s)",
                 bytes_fetched,
                 open_chunks.chunk_reflist.path.ToString().c_str());
        return -bytes_fetched;
      }
      overall_bytes_fetched += bytes_fetched;

      // Proceed to the next chunk to keep on reading data
      ++chunk_idx;
      offset_in_chunk = 0;
    } while ((overall_bytes_fetched < size) &&
             (chunk_idx < chunk_list->size()));
    return overall_bytes_fetched;
  } else {
    return fetcher_->cache_mgr()->Pread(fd, buf, size, off);
  }
}

int cvmfs_context::Close(int fd) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_close on file number: %d", fd);
  if (fd & kFdChunked) {
    const int chunk_handle = fd & ~kFdChunked;
    SimpleChunkTables::OpenChunks open_chunks = chunk_tables_.Get(chunk_handle);
    if (open_chunks.chunk_reflist.list == NULL)
      return -EBADF;
    if (open_chunks.chunk_fd->fd != -1)
      cvmfs_globals::Instance()->cache_mgr()->Close(open_chunks.chunk_fd->fd);
    chunk_tables_.Release(chunk_handle);
  } else {
    cvmfs_globals::Instance()->cache_mgr()->Close(fd);
  }
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

  atomic_init32(&open_dirs_);
}
