/**
 * CernVM-FS is a FUSE module which implements an HTTP read-only filesystem.
 * The original idea is based on GROW-FS.
 *
 * CernVM-FS shows a remote HTTP directory as local file system.  The client
 * sees all available files.  On first access, a file is downloaded and
 * cached locally.  All downloaded pieces are verified with SHA1.
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

#include "cvmfs_config.h"

#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifndef __APPLE__
#include <sys/statfs.h>
#endif
#include <sys/wait.h>
#include <sys/errno.h>
#include <sys/mount.h>
#include <sys/file.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <sys/xattr.h>

#include <openssl/crypto.h>
#include <google/dense_hash_map>
#include "MurmurHash2.h"

#include <cstdlib>
#include <cstring>
#include <csignal>
#include <ctime>
#include <cassert>
#include <cstdio>

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <functional>

#include "platform.h"
#include "logging.h"
#include "tracer.h"
#include "download.h"
#include "cache.h"
#include "hash.h"
#include "talk.h"
#include "monitor.h"
#include "signature.h"
#include "quota.h"
#include "util.h"
#include "atomic.h"
#include "lru.h"
#include "peers.h"
#include "dirent.h"
#include "compression.h"
#include "duplex_sqlite3.h"
#include "shortstring.h"
#include "smalloc.h"
#include "globals.h"
#include "libcvmfs_int.h"

using namespace std;  // NOLINT

namespace cvmfs {

const unsigned int kPathCacheSize = 4800;
const unsigned int kMd5pathCacheSize = 32000;

const unsigned int kShortTermTTL = 180;  /**< If catalog reload fails, try again
                                              in 3 minutes */
const time_t kIndefiniteDeadline = time_t(-1);

const int kMaxInitIoDelay = 32; /**< Maximum start value for exponential
                                     backoff */
const int kMaxIoDelay = 2000; /**< Maximum 2 seconds */
const int kForgetDos = 10000; /**< Clear DoS memory after 10 seconds */
/**
 * Prevent DoS attacks on the Squid server
 */
static struct {
  time_t timestamp;
  int delay;
} previous_io_error_;

bool foreground_ = false;
string *mountpoint_ = NULL;
string *cachedir_ = NULL;
string relative_cachedir; /* path to cachedir, relative to current working dir */
string *tracefile_ = NULL;
string *repository_name_ = NULL;  /**< Expected repository name,
                                       e.g. atlas.cern.ch */
pid_t pid_ = 0;  /**< will be set after deamon() */
time_t boot_time_;
pthread_mutex_t lock_max_ttl_ = PTHREAD_MUTEX_INITIALIZER;
cache::CatalogManager *catalog_manager_;
lru::Md5PathCache *md5path_cache_ = NULL;

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
const int kNumReservedFd = 512;  /**< Number of reserved file descriptors for
                                      internal use */


unsigned GetMaxTTL() {
  return 0;
}


void SetMaxTTL(const unsigned value) {
}


unsigned GetRevision() {
  return catalog_manager_->GetRevision();
};


std::string GetOpenCatalogs() {
  return catalog_manager_->PrintHierarchy();
}


void ResetErrorCounters() {
  atomic_init32(&num_io_error_);
}


void GetLruStatistics(lru::Statistics *inode_stats, lru::Statistics *path_stats,
                      lru::Statistics *md5path_stats)
{
  *md5path_stats = md5path_cache_->statistics();
}


catalog::Statistics GetCatalogStatistics() {
  return catalog_manager_->statistics();
}

string GetCertificateStats() {
  return catalog_manager_->GetCertificateStats();
}

string GetFsStats() {
  return "lookup(all): " + StringifyInt(atomic_read64(&num_fs_lookup_)) + "  " +
    "lookup(negative): " + StringifyInt(atomic_read64(&num_fs_lookup_negative_))
      + "  " +
    "stat(): " + StringifyInt(atomic_read64(&num_fs_stat_)) + "  " +
    "open(): " + StringifyInt(atomic_read64(&num_fs_open_)) + "  " +
    "diropen(): " + StringifyInt(atomic_read64(&num_fs_dir_open_)) + "  " +
    "read(): " + StringifyInt(atomic_read64(&num_fs_read_)) + "  " +
    "readlink(): " + StringifyInt(atomic_read64(&num_fs_readlink_)) + "\n";
}


/**
 * If there is a new catalog version, load it.
 */
catalog::LoadError RemountStart() {
  catalog::LoadError retval = catalog_manager_->Remount(true);
  if (retval == catalog::kLoadNew) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "new catalog revision available");

    LogCvmfs(kLogCvmfs, kLogDebug, "applying new catalog");
    md5path_cache_->Pause();
    md5path_cache_->Drop();
    retval = catalog_manager_->Remount(false);
    md5path_cache_->Resume();
    if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace) ||
        catalog_manager_->offline_mode())
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "reload/finish failed");
    }
  }
  return retval;
}


static bool GetDirentForPath(const PathString &path,
                             catalog::DirectoryEntry *dirent)
{
  if( path.GetLength() == 1 && path.GetChars()[0] == '/' ) {
    // root path is expected to be "", not "/"
    PathString p;
    return GetDirentForPath(p,dirent);
  }
  hash::Md5 md5path(path.GetChars(), path.GetLength());
  if (md5path_cache_->Lookup(md5path, dirent))
    return dirent->GetSpecial() != catalog::kDirentNegative;

  // Lookup inode in catalog TODO: not twice md5 calculation
  if (catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent)) {
    md5path_cache_->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  md5path_cache_->InsertNegative(md5path);
  return false;
}

/**
 * Removes a file from local cache
 */
int ClearFile(const string &path) {
	// not implemented, but required for linking
  return 0;
}

}  // namespace cvmfs


bool g_foreground = false;
bool g_single_threaded = false;

// Making OpenSSL (libcrypto) thread-safe
pthread_mutex_t *gLibcryptoLocks;

static void CallbackLibcryptoLock(int mode, int type,
                                  const char *file, int line) {
  (void)file;
  (void)line;

  int retval;

  if (mode & CRYPTO_LOCK) {
    retval = pthread_mutex_lock(&(gLibcryptoLocks[type]));
  } else {
    retval = pthread_mutex_unlock(&(gLibcryptoLocks[type]));
  }
  assert(retval == 0);
}

static unsigned long CallbackLibcryptoThreadId() {
  return platform_gettid();
}

static void SetupLibcryptoMt() {
  gLibcryptoLocks = static_cast<pthread_mutex_t *>(OPENSSL_malloc(
                      CRYPTO_num_locks() * sizeof(pthread_mutex_t)));
  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    int retval = pthread_mutex_init(&(gLibcryptoLocks[i]), NULL);
    assert(retval == 0);
  }

  CRYPTO_set_id_callback(CallbackLibcryptoThreadId);
  CRYPTO_set_locking_callback(CallbackLibcryptoLock);
}

static void CleanupLibcryptoMt(void) {
  CRYPTO_set_locking_callback(NULL);
  for (int i = 0; i < CRYPTO_num_locks(); ++i)
    pthread_mutex_destroy(&(gLibcryptoLocks[i]));

  OPENSSL_free(gLibcryptoLocks);
}

namespace cvmfs {

int fd_lockfile;
static void *sqlite_scratch;
static void *sqlite_page_cache;
static bool options_ready;
static bool download_ready;
static bool cache_ready;
static bool peers_ready;
static bool monitor_ready;
static bool signature_ready;
static bool quota_ready;
static bool catalog_ready;
static bool talk_ready;
static bool running_created;


/**
 * Off we go
 */

int cvmfs_int_init(
  const std::string &cvmfs_opts_hostname, /* url of repository */
  const std::string &cvmfs_opts_proxies,
  const std::string &cvmfs_opts_repo_name,
  const std::string &cvmfs_opts_mountpoint,
  const std::string &cvmfs_opts_pubkey,
  const std::string &cvmfs_opts_cachedir,
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
  const std::string &cvmfs_opts_whitelist,
  int cvmfs_opts_nofiles,
  bool cvmfs_opts_enable_talk,
  bool cvmfs_opts_enable_monitor
) {

  int retval;

  fd_lockfile = -1;
  sqlite_scratch = NULL;
  sqlite_page_cache = NULL;
  options_ready = false;
  download_ready = false;
  cache_ready = false;
  peers_ready = false;
  monitor_ready = false;
  signature_ready = false;
  quota_ready = false;
  catalog_ready = false;
  talk_ready = false;
  running_created = false;

  cvmfs::boot_time_ = time(NULL);
  SetupLibcryptoMt();

  // Fill cvmfs option variables from arguments
  cvmfs::mountpoint_ = new string(cvmfs_opts_mountpoint);
  cvmfs::cachedir_ = new string(cvmfs_opts_cachedir);
  if (cvmfs_opts_cd_to_cachedir) {
    cvmfs::relative_cachedir = ".";
  }
  else {
    cvmfs::relative_cachedir = *cvmfs::cachedir_;
  }
  cvmfs::tracefile_ = new string(cvmfs_opts_tracefile);
  cvmfs::repository_name_ = new string(cvmfs_opts_repo_name);
  g_uid = getuid();
  g_gid = getgid();
  options_ready = true;

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

  // Runtime counters
  atomic_init64(&cvmfs::num_fs_open_);
  atomic_init64(&cvmfs::num_fs_dir_open_);
  atomic_init64(&cvmfs::num_fs_lookup_);
  atomic_init64(&cvmfs::num_fs_lookup_negative_);
  atomic_init64(&cvmfs::num_fs_stat_);
  atomic_init64(&cvmfs::num_fs_read_);
  atomic_init64(&cvmfs::num_fs_readlink_);
  atomic_init32(&cvmfs::num_io_error_);
  cvmfs::previous_io_error_.timestamp = 0;
  cvmfs::previous_io_error_.delay = 0;

  // Logging
  SetLogSyslogLevel(cvmfs_opts_syslog_level);
  SetLogSyslogPrefix(*cvmfs::repository_name_);
  if (!cvmfs_opts_logfile.empty())
    SetLogDebugFile(string(cvmfs_opts_logfile));

  // Maximum number of open files
  if (cvmfs_opts_nofiles) {
    if (cvmfs_opts_nofiles < 0) {
      PrintError("number of open files must be a positive number");
      goto cvmfs_cleanup;
    }
    struct rlimit rpl;
    memset(&rpl, 0, sizeof(rpl));
    getrlimit(RLIMIT_NOFILE, &rpl);
    if (rpl.rlim_max < (unsigned)cvmfs_opts_nofiles)
      rpl.rlim_max = cvmfs_opts_nofiles;
    rpl.rlim_cur = cvmfs_opts_nofiles;
    if (setrlimit(RLIMIT_NOFILE, &rpl) != 0) {
      PrintError("Failed to set maximum number of open files, "
                 "insufficient permissions");
      goto cvmfs_cleanup;
    }
  }

  // Create cache directory, if necessary
  if (!MkdirDeep(*cvmfs::cachedir_, 0700)) {
    PrintError("cannot create cache directory " + *cvmfs::cachedir_);
    goto cvmfs_cleanup;
  }

  peers_ready = true;

  // Try to jump to cache directory.  This tests, if it is accassible.
  // Also, it brings speed later on.
  if (cvmfs_opts_cd_to_cachedir && chdir(cvmfs::cachedir_->c_str()) != 0) {
    PrintError("cache directory " + *cvmfs::cachedir_ + " is unavailable");
    goto cvmfs_cleanup;
  }

  // Create lock file and running sentinel
  fd_lockfile = LockFile(relative_cachedir + "/lock." + *cvmfs::repository_name_);
  if (fd_lockfile < 0) {
    PrintError("could not acquire lock (" + StringifyInt(errno) + ")");
    goto cvmfs_cleanup;
  }
  {
    platform_stat64 info;
    if (platform_stat((relative_cachedir + "/running." + *cvmfs::repository_name_).c_str(),
                      &info) == 0)
    {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "looks like cvmfs has been "
               "crashed previously, rebuilding cache database");
      cvmfs_opts_rebuild_cachedb = 1;
    }
  }
  retval = open((relative_cachedir + "/running." + *cvmfs::repository_name_).c_str(),
                O_RDONLY | O_CREAT, 0600);
  if (retval < 0) {
    PrintError("could not open running sentinel (" + StringifyInt(errno) + ")");
    goto cvmfs_cleanup;
  }
  close(retval);
  running_created = true;

  // Creates a set of cache directories (256 directories named 00..ff)
  if (!cache::Init(relative_cachedir)) {
    PrintError("Failed to setup cache in " + *cvmfs::cachedir_ +
               ": " + strerror(errno));
    goto cvmfs_cleanup;
  }
  cache_ready = true;

  // Init quota / managed cache
  if (cvmfs_opts_quota_limit < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "unlimited cache size");
    cvmfs_opts_quota_limit = -1;
    cvmfs_opts_quota_threshold = 0;
  } else {
    cvmfs_opts_quota_limit *= 1024*1024;
    cvmfs_opts_quota_threshold *= 1024*1024;
  }
  if (!quota::Init(relative_cachedir, (uint64_t)cvmfs_opts_quota_limit,
                   (uint64_t)cvmfs_opts_quota_threshold,
                   cvmfs_opts_rebuild_cachedb))
  {
    PrintError("Failed to initialize lru cache");
    goto cvmfs_cleanup;
  }
  quota_ready = true;

  if (quota::GetSize() > quota::GetCapacity()) {
    PrintWarning("your cache is already beyond quota size, cleaning up");
    if (!quota::Cleanup(cvmfs_opts_quota_threshold)) {
      PrintWarning("Failed to clean up");
      goto cvmfs_cleanup;
    }
  }
  if (cvmfs_opts_quota_limit) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: quota initialized, current size %luMB",
             quota::GetSize()/(1024*1024));
  }

  // Monitor, check for maximum number of open files
  if (cvmfs_opts_enable_monitor) {
    if (!monitor::Init(relative_cachedir, true)) {
      PrintError("failed to initialize watchdog.");
      goto cvmfs_cleanup;
    }
    cvmfs::max_open_files_ = monitor::GetMaxOpenFiles();
    monitor_ready = true;
  }
  else {
    cvmfs::max_open_files_ = 0;
  }
  atomic_init32(&cvmfs::open_files_);
  atomic_init32(&cvmfs::open_dirs_);

  // Control & command interface
  if (cvmfs_opts_enable_talk) {
    if (!talk::Init(relative_cachedir)) {
      PrintError("failed to initialize talk socket (" + StringifyInt(errno) +
                 ")");
      goto cvmfs_cleanup;
    }
    talk_ready = true;
  }

  // Network initialization
  download::Init(16);
  download::SetHostChain(string(cvmfs_opts_hostname));
  download::SetProxyChain(cvmfs_opts_proxies);
  download::SetTimeout(cvmfs_opts_timeout, cvmfs_opts_timeout_direct);
  download_ready = true;

  signature::Init();
  if (!signature::LoadPublicRsaKeys(cvmfs_opts_pubkey))
  {
    PrintError("failed to load public key(s)");
    goto cvmfs_cleanup;
  } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: using public key(s) %s",
               JoinStrings(
                 SplitString(cvmfs_opts_pubkey, ':'), ", ").c_str());
  }
  signature_ready = true;
  if (!cvmfs_opts_blacklist.empty()) {
    if (!signature::LoadBlacklist(cvmfs_opts_blacklist)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "failed to load blacklist");
      goto cvmfs_cleanup;
    }
  }

  // Load initial file catalog
  cvmfs::catalog_manager_ = new
    cache::CatalogManager(*cvmfs::repository_name_,
                          cvmfs_opts_ignore_signature);
  if (!cvmfs_opts_root_hash.empty()) {
    retval = cvmfs::catalog_manager_->InitFixed(
      hash::Any(hash::kSha1, hash::HexPtr(string(cvmfs_opts_root_hash))));
  } else {
    retval = cvmfs::catalog_manager_->Init();
  }
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to initialize root file catalog");
    goto cvmfs_cleanup;
  }
  catalog_ready = true;

  // Set fuse callbacks, remove url from arguments
  LogCvmfs(kLogCvmfs, kLogSyslog,
           "CernVM-FS: linking %s to repository %s",
           cvmfs::mountpoint_->c_str(), cvmfs::repository_name_->c_str());

  cvmfs::md5path_cache_ = new lru::Md5PathCache(cvmfs::kMd5pathCacheSize);

  cvmfs_int_spawn();
  return 0;

cvmfs_cleanup:
   cvmfs_int_fini();
   return 1;
}

/**
 * Do after-daemon() initialization
 */
void cvmfs_int_spawn() {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_spawn");

  pid_ = getpid();
  if (monitor_ready) {
    monitor::Spawn();
  }
  download::Spawn();
  quota::Spawn();
  if (talk_ready) {
    talk::Spawn();
  }

  if (*tracefile_ != "")
    tracer::Init(8192, 7000, *tracefile_);
  else
    tracer::InitNull();

}

void cvmfs_int_fini() {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_destroy");

  delete cvmfs::catalog_manager_;
  delete cvmfs::md5path_cache_;
  cvmfs::catalog_manager_ = NULL;
  cvmfs::md5path_cache_ = NULL;

  if (signature_ready) signature::Fini();
  if (download_ready) download::Fini();
  if (talk_ready) talk::Fini();
  if (monitor_ready) monitor::Fini();
  if (quota_ready) quota::Fini();
  if (cache_ready) cache::Fini();
  if (running_created) unlink((relative_cachedir + "/running." + *cvmfs::repository_name_).c_str());
  if (fd_lockfile >= 0) UnlockFile(fd_lockfile);
  if (peers_ready) peers::Fini();
  tracer::Fini();

  sqlite3_shutdown();
  free(sqlite_page_cache);
  free(sqlite_scratch);
  sqlite_page_cache = NULL;
  sqlite_scratch = NULL;

  CleanupLibcryptoMt();
}

int cvmfs_open(const char *c_path)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on path: %s", c_path);

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;
  path.Assign(c_path, strlen(c_path));

  const bool found = GetDirentForPath(path, &dirent);

  if (!found) {
    return -ENOENT;
  }

  fd = cache::Fetch(dirent, string(path.GetChars(), path.GetLength()));  // TODO
  atomic_inc64(&num_fs_open_);

  if (fd >= 0) {
    if (atomic_xadd32(&open_files_, 1) <
        (static_cast<int>(max_open_files_))-kNumReservedFd || max_open_files_==0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
      return fd;
    } else {
      if (close(fd) == 0) atomic_dec32(&open_files_);
      LogCvmfs(kLogCvmfs, kLogSyslog, "open file descriptor limit exceeded");
      return -EMFILE;
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "failed to open path: %s, CAS key %s, error code %d",
             c_path, dirent.checksum().ToString().c_str(), errno);
    if (errno == EMFILE) {
      return -EMFILE;
    }
  }

  // Prevent Squid DoS
  // TODO: move to download
  time_t now = time(NULL);
  if (now - previous_io_error_.timestamp < kForgetDos) {
    usleep(previous_io_error_.delay*1000);
    if (previous_io_error_.delay < kMaxIoDelay)
      previous_io_error_.delay *= 2;
  } else {
    // Initial delay
    previous_io_error_.delay = (random() % (kMaxInitIoDelay-1)) + 2;
  }
  previous_io_error_.timestamp = now;

  atomic_inc32(&num_io_error_);
  return fd;
}

int cvmfs_close(int fd)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_close on file number: %d",
           fd);

  if (close(fd) == 0) atomic_dec32(&open_files_);

  return 0;
}

/**
 * Transform a cvmfs dirent into a struct stat.
 */
int cvmfs_getattr(const char *c_path, struct stat *info)
{
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

/**
 * Reads a symlink from the catalog.  Environment variables are expanded.
 */
int cvmfs_readlink(const char *c_path, char *buf, size_t size)
{
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

  unsigned len = (dirent.symlink().GetLength() >= size) ? size : dirent.symlink().GetLength()+1;
  strncpy(buf, dirent.symlink().c_str(), len-1);
  buf[len-1] = '\0';

  return 0;
}

static void append_string_to_list(char const *str,char ***buf,size_t *listlen,size_t *buflen)
{
   if( *listlen + 1 >= *buflen ) {
       size_t newbuflen = (*listlen)*2 + 5;
       *buf = (char **)realloc(*buf,sizeof(char *)*newbuflen);
       assert( *buf );
       *buflen = newbuflen;
       assert( *listlen < *buflen );
   }
   if( str ) {
       (*buf)[(*listlen)] = strdup(str);
       // null-terminate the list
       (*buf)[++(*listlen)] = NULL;
   }
   else {
       (*buf)[(*listlen)] = NULL;
   }
}

int cvmfs_listdir(const char *c_path,char ***buf,size_t *buflen)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_listdir on path: %s", c_path);

  if( c_path[0] == '/' && c_path[1] == '\0' ) {
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
  append_string_to_list(NULL,buf,&listlen,buflen);

  // Build listing

  // Add current directory link
  append_string_to_list(".",buf,&listlen,buflen);

  // Add parent directory link
  catalog::DirectoryEntry p;
  if (d.inode() != catalog_manager_->GetRootInode()) {
    append_string_to_list("..",buf,&listlen,buflen);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  if (!catalog_manager_->ListingStat(path, &listing_from_catalog)) {
    return -EIO;
  }
  for (catalog::StatEntryList::const_iterator i = listing_from_catalog.begin(),
       iEnd = listing_from_catalog.end(); i != iEnd; ++i)
  {
    append_string_to_list(i->name.c_str(),buf,&listlen,buflen);
  }

  return 0;
}

int cvmfs_statfs(const char *c_path __attribute__((unused)), struct statvfs *info)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_statfs on path: %s", c_path);

  // If we return 0 it will cause the fs to be ignored in "df"
  memset(info, 0, sizeof(*info));

  // Unmanaged cache
  if (quota::GetCapacity() == 0) {
    return 0;
  }

  uint64_t available = 0;
  uint64_t size = quota::GetSize();
  info->f_bsize = 1;

  if (quota::GetCapacity() == (uint64_t)(-1)) {
    // Unrestricted cache, look at free space on cache dir fs
    struct statfs cache_buf;
    if (statfs(relative_cachedir.c_str(), &cache_buf) == 0) {
      available = cache_buf.f_bavail * cache_buf.f_bsize;
      info->f_blocks = size + available;
    } else {
      info->f_blocks = size;
    }
  } else {
    // Take values from LRU module
    info->f_blocks = quota::GetCapacity();
    available = quota::GetCapacity() - size;
  }

  info->f_bfree = info->f_bavail = available;

  return 0;
}

}
