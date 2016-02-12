/**
 * This file is part of the CernVM File System.
 *
 * CernVM-FS is a FUSE module which implements an HTTP read-only filesystem.
 * The original idea is based on GROW-FS.
 *
 * CernVM-FS shows a remote HTTP directory as local file system.  The client
 * sees all available files.  On first access, a file is downloaded and
 * cached locally.  All downloaded pieces are verified by a cryptographic
 * content hash.
 *
 * To do so, a directory hive has to be transformed into a CVMFS2
 * "repository".  This can be done by the CernVM-FS server tools.
 *
 * This preparation of directories is transparent to web servers and
 * web proxies.  They just serve static content, i.e. arbitrary files.
 * Any HTTP server should do the job.  We use Apache + Squid.  Serving
 * files from the memory of a web proxy brings a significant performance
 * improvement.
 */

// TODO(jblomer): the file system root should probably always return 1 for an
// inode.  See also integration test #23.

#define ENOATTR ENODATA  /**< instead of including attr/xattr.h */
#define FUSE_USE_VERSION 26
#define __STDC_FORMAT_MACROS

// sys/xattr.h conflicts with linux/xattr.h and needs to be loaded very early
#include <sys/xattr.h>  // NOLINT

#include "cvmfs_config.h"
#include "cvmfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>
#include <google/dense_hash_map>
#include <inttypes.h>
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

#include <algorithm>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "auto_umount.h"
#include "backoff.h"
#include "cache.h"
#include "catalog_mgr_client.h"
#include "clientctx.h"
#include "compat.h"
#include "compression.h"
#include "directory_entry.h"
#include "download.h"
#include "duplex_sqlite3.h"
#include "fetch.h"
#include "file_chunk.h"
#include "globals.h"
#include "glue_buffer.h"
#include "hash.h"
#include "history_sqlite.h"
#include "loader.h"
#include "logging.h"
#include "lru.h"
#include "manifest_fetch.h"
#include "monitor.h"
#include "nfs_maps.h"
#include "options.h"
#include "platform.h"
#include "quota.h"
#include "quota_listener.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "sqlitevfs.h"
#include "statistics.h"
#include "talk.h"
#include "tracer.h"
#include "util.h"
#include "util_concurrency.h"
#include "uuid.h"
#include "voms_authz/voms_cred.h"
#include "wpad.h"
#include "xattr.h"

#include "voms_authz/voms_authz.h"

#ifdef FUSE_CAP_EXPORT_SUPPORT
#define CVMFS_NFS_SUPPORT
#else
#warning "No NFS support, Fuse too old"
#endif

using namespace std;  // NOLINT

namespace cvmfs {

const char *kDefaultCachedir = "/var/lib/cvmfs/default";
const unsigned kDefaultTimeout = 2;
const unsigned kDefaultLowSpeedLimit = 1024;
const double kDefaultKCacheTimeout = 60.0;
const unsigned kReloadSafetyMargin = 500;  // in milliseconds
const unsigned kDefaultNumConnections = 16;
const uint64_t kDefaultMemcache = 16*1024*1024;  // 16M RAM for meta-data caches
const uint64_t kDefaultCacheSizeMb = 1024*1024*1024;  // 1G
/**
 * If catalog reload fails, try again in 3 minutes
 */
const unsigned int kShortTermTTL = 180;
const time_t kIndefiniteDeadline = time_t(-1);

BackoffThrottle *backoff_throttle_;

/**
 * Stores the initial catalog revision (in order to detect overflows) and
 * the incarnation (number of reloads) of the Fuse module
 */
struct InodeGenerationInfo {
  InodeGenerationInfo() {
    version = 2;
    initial_revision = 0;
    incarnation = 0;
    overflow_counter = 0;
    inode_generation = 0;
  }
  unsigned version;
  uint64_t initial_revision;
  uint32_t incarnation;
  uint32_t overflow_counter;  // not used any more
  uint64_t inode_generation;
};
InodeGenerationInfo inode_generation_info_;

/**
 * For cvmfs_opendir / cvmfs_readdir
 * TODO: use mmap for very large listings
 */
struct DirectoryListing {
  char *buffer;  /**< Filled by fuse_add_direntry */

  // Not really used anymore.  But directory listing needs to be migrated during
  // hotpatch. If buffer is allocated by smmap, capacity is zero.
  size_t size;
  size_t capacity;

  DirectoryListing() : buffer(NULL), size(0), capacity(0) { }
};

const loader::LoaderExports *loader_exports_ = NULL;
bool foreground_ = false;
bool nfs_maps_ = false;
string *mountpoint_ = NULL;
string *cachedir_ = NULL;
string *nfs_shared_dir_ = NULL;
string *tracefile_ = NULL;
/**
 * Expected repository name, e.g. atlas.cern.ch
 */
string *repository_name_ = NULL;
string *repository_tag_ = NULL;
pid_t pid_ = 0;  /**< will be set after deamon() */
time_t boot_time_;
unsigned max_ttl_ = 0;  /**< unit: seconds */
pthread_mutex_t lock_max_ttl_ = PTHREAD_MUTEX_INITIALIZER;
catalog::InodeGenerationAnnotation *inode_annotation_ = NULL;
catalog::ClientCatalogManager *catalog_manager_ = NULL;
quota::ListenerHandle *watchdog_listener_ = NULL;
quota::ListenerHandle *unpin_listener_ = NULL;
signature::SignatureManager *signature_manager_ = NULL;
download::DownloadManager *download_manager_ = NULL;
download::DownloadManager *external_download_manager_ = NULL;
cache::CacheManager *cache_manager_ = NULL;
Fetcher *fetcher_ = NULL;
Fetcher *external_fetcher_ = NULL;
lru::InodeCache *inode_cache_ = NULL;
lru::PathCache *path_cache_ = NULL;
lru::Md5PathCache *md5path_cache_ = NULL;
glue::InodeTracker *inode_tracker_ = NULL;
OptionsManager *options_manager_ = NULL;

double kcache_timeout_ = kDefaultKCacheTimeout;
bool fixed_catalog_ = false;
bool volatile_repository_ = false;
/**
 * If true, synthetic extended attributes (e.g. "user.pid") are excluded from
 * listing.  They are still retrievable if the attribute key is known.  This
 * helps if cvmfs is a read-only layer in a union file system stack where the
 * synthetic attributes should not be copied up.
 */
bool hide_magic_xattrs_ = false;

/**
 * in maintenance mode, cache timeout is 0 and catalogs are not reloaded
 */
atomic_int32 maintenance_mode_;
atomic_int32 catalogs_expired_;
atomic_int32 drainout_mode_;
atomic_int32 reload_critical_section_;
time_t drainout_deadline_;
time_t catalogs_valid_until_;

/**
 * Caches if there is a VOMS requirement set in the root catalog.  Set on
 * initial mount and on remount.
 */
bool has_voms_authz_;
std::string *voms_authz_;

typedef google::dense_hash_map<uint64_t, DirectoryListing,
                               hash_murmur<uint64_t> >
        DirectoryHandles;
DirectoryHandles *directory_handles_ = NULL;
pthread_mutex_t lock_directory_handles_ = PTHREAD_MUTEX_INITIALIZER;
uint64_t next_directory_handle_ = 0;

// contains inode to chunklist and handle to fd maps
ChunkTables *chunk_tables_;

perf::Statistics *statistics_ = NULL;
perf::Counter *n_fs_open_ = NULL;
perf::Counter *n_fs_dir_open_ = NULL;
perf::Counter *n_fs_lookup_ = NULL;
perf::Counter *n_fs_lookup_negative_ = NULL;
perf::Counter *n_fs_stat_ = NULL;
perf::Counter *n_fs_read_ = NULL;
perf::Counter *n_fs_readlink_ = NULL;
perf::Counter *n_fs_forget_ = NULL;
perf::Counter *n_io_error_ = NULL;
/**
 *  number of currently open files by Fuse calls
 */
perf::Counter *no_open_files_ = NULL;
/**
 * number of currently open directories
 */
perf::Counter *no_open_dirs_ = NULL;
unsigned max_open_files_; /**< maximum allowed number of open files */
/**
 * Number of reserved file descriptors for internal use
 */
const int kNumReservedFd = 512;

/**
 * Ensures that within a callback all operations take place on the same
 * catalog revision.
 */
class RemountFence : public SingleCopy {
 public:
  RemountFence() {
    atomic_init64(&counter_);
    atomic_init32(&blocking_);
  }
  void Enter() {
    while (atomic_read32(&blocking_)) {
      SafeSleepMs(100);
    }
    atomic_inc64(&counter_);
  }
  void Leave() {
    atomic_dec64(&counter_);
  }
  void Block() {
    atomic_cas32(&blocking_, 0, 1);
    while (atomic_read64(&counter_) > 0) {
      SafeSleepMs(100);
    }
  }
  void Unblock() {
    atomic_cas32(&blocking_, 1, 0);
  }

 private:
  atomic_int64 counter_;
  atomic_int32 blocking_;
};
RemountFence *remount_fence_;


/**
 * The thread that triggers the reload of the root catalog is informed through
 * this pipe by the alarm signal handler when the TTL expires.
 */
int pipe_remount_trigger_[2];

/**
 * Triggers `RemountCheck()` when the repository TTL expires.
 */
pthread_t *thread_remount_trigger_ = NULL;


unsigned GetMaxTTL() {
  pthread_mutex_lock(&lock_max_ttl_);
  const unsigned current_max = max_ttl_/60;
  pthread_mutex_unlock(&lock_max_ttl_);

  return current_max;
}


void SetMaxTTL(const unsigned value_minutes) {
  pthread_mutex_lock(&lock_max_ttl_);
  max_ttl_ = value_minutes*60;
  pthread_mutex_unlock(&lock_max_ttl_);
}


static unsigned GetEffectiveTTL() {
  const unsigned max_ttl = GetMaxTTL()*60;
  const unsigned catalog_ttl = catalog_manager_->GetTTL();

  return max_ttl ? std::min(max_ttl, catalog_ttl) : catalog_ttl;
}


static inline double GetKcacheTimeout() {
  if (atomic_read32(&drainout_mode_) || atomic_read32(&maintenance_mode_))
    return 0.0;
  return kcache_timeout_;
}


void GetReloadStatus(bool *drainout_mode, bool *maintenance_mode) {
  *drainout_mode = atomic_read32(&drainout_mode_);
  *maintenance_mode = atomic_read32(&maintenance_mode_);
}


unsigned GetRevision() {
  return catalog_manager_->GetRevision();
}


std::string GetOpenCatalogs() {
  return catalog_manager_->PrintHierarchy();
}


void ResetErrorCounters() {
  n_io_error_->Set(0);
}


static bool UseWatchdog() {
  if (loader_exports_ == NULL || loader_exports_->version < 2) {
    return true;  // spawn watchdog by default
                  // Note: with library versions before 2.1.8 it might not
                  //       create stack traces properly in all cases
  }

  return !loader_exports_->disable_watchdog;
}


static void AlarmReload(int signal __attribute__((unused)),
                        siginfo_t *siginfo __attribute__((unused)),
                        void *context __attribute__((unused)))
{
  atomic_cas32(&catalogs_expired_, 0, 1);
  if (pipe_remount_trigger_[1] >= 0) {
    char c = 'T';
    WritePipe(pipe_remount_trigger_[1], &c, 1);
  }
}


/**
 * If there is a new catalog version, switches to drainout mode.
 * lookup or getattr will take care of actual remounting once the caches are
 * drained out.
 */
catalog::LoadError RemountStart() {
  catalog::LoadError retval = catalog_manager_->Remount(true);
  if (retval == catalog::kLoadNew) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "new catalog revision available, draining out meta-data caches");
    unsigned safety_margin = kReloadSafetyMargin/1000;
    if (safety_margin == 0)
      safety_margin = 1;
    drainout_deadline_ =
      time(NULL) + static_cast<int>(kcache_timeout_) + safety_margin;
    atomic_cas32(&drainout_mode_, 0, 1);
  }
  return retval;
}


/**
 * If the caches are drained out, a new catalog revision is applied and
 * kernel caches are activated again.
 */
static void RemountFinish() {
  if (!atomic_cas32(&reload_critical_section_, 0, 1))
    return;
  if (!atomic_read32(&drainout_mode_)) {
    atomic_cas32(&reload_critical_section_, 1, 0);
    return;
  }

  if (time(NULL) > drainout_deadline_) {
    LogCvmfs(kLogCvmfs, kLogDebug, "caches drained out, applying new catalog");

    // No new inserts into caches
    inode_cache_->Pause();
    path_cache_->Pause();
    md5path_cache_->Pause();
    inode_cache_->Drop();
    path_cache_->Drop();
    md5path_cache_->Drop();

    // Ensure that all Fuse callbacks left the catalog query code
    remount_fence_->Block();
    catalog::LoadError retval = catalog_manager_->Remount(false);
    if (inode_annotation_) {
      inode_generation_info_.inode_generation =
        inode_annotation_->GetGeneration();
    }
    volatile_repository_ = catalog_manager_->GetVolatileFlag();
    has_voms_authz_ = catalog_manager_->GetVOMSAuthz(voms_authz_);
    remount_fence_->Unblock();

    inode_cache_->Resume();
    path_cache_->Resume();
    md5path_cache_->Resume();

    atomic_cas32(&drainout_mode_, 1, 0);
    if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace) ||
        catalog_manager_->offline_mode())
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "reload/finish failed, "
               "applying short term TTL");
      alarm(kShortTermTTL);
      catalogs_valid_until_ = time(NULL) + kShortTermTTL;
    } else {
      LogCvmfs(kLogCvmfs, kLogSyslog, "switched to catalog revision %d",
               catalog_manager_->GetRevision());
      alarm(GetEffectiveTTL());
      catalogs_valid_until_ = time(NULL) + GetEffectiveTTL();
    }
  }

  atomic_cas32(&reload_critical_section_, 1, 0);
}


/**
 * Runs at the beginning of lookup, checks if a previously started remount needs
 * to be finished or starts a new remount if the TTL timer has been fired.
 */
static void RemountCheck() {
  if (atomic_read32(&maintenance_mode_) == 1)
    return;
  RemountFinish();

  if (atomic_cas32(&catalogs_expired_, 1, 0)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "catalog TTL expired, reload");
    catalog::LoadError retval = RemountStart();
    if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "reload failed (%s), "
                                     "applying short term TTL",
               catalog::Code2Ascii(retval));
      alarm(kShortTermTTL);
      catalogs_valid_until_ = time(NULL) + kShortTermTTL;
    } else if (retval == catalog::kLoadUp2Date) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "catalog up to date, applying effective TTL");
      alarm(GetEffectiveTTL());
      catalogs_valid_until_ = time(NULL) + GetEffectiveTTL();
    }
  }
}


static bool CheckVoms(const fuse_ctx &fctx) {
  if (has_voms_authz_) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Got VOMS authz %s from filesystem "
             "properties", voms_authz_->c_str());
  }

  // Get VOMS information, if any.  If VOMS authz is present and VOMS is
  // not compiled in, then deny authorization.
  if ((fctx.uid != 0) && voms_authz_->size()) {
//  #ifdef VOMS_AUTHZ
//      return CheckVOMSAuthz(&fctx, *voms_authz_);
//  #else
    LogCvmfs(kLogCvmfs, kLogSyslogWarn | kLogDebug,  "VOMS requirements found "
              "in catalog but client compiled without VOMS support");
    return false;
//  #endif
  }
  return true;
}

// TODO(jblomer): the remount functions need to move into a separate entity
/**
 * Gets triggered by the alarm timer to start `RemountCheck()`.  This can't
 * be called from the alarm timer because of missing signal safety.
 */
static void *MainRemountTrigger(void *data) {
  char c;
  LogCvmfs(kLogCvmfs, kLogDebug, "starting remount trigger");
  while (true) {
    ReadPipe(pipe_remount_trigger_[0], &c, 1);
    if (c == 'Q')
      break;

    // We don't need to call this if we are already draining the caches
    if (!atomic_read32(&drainout_mode_)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "trigger remount of idle mount point");
      RemountCheck();
    }
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "stopping remount trigger");
  return NULL;
}


static bool GetDirentForInode(const fuse_ino_t ino,
                              catalog::DirectoryEntry *dirent)
{
  // Lookup inode in cache
  if (inode_cache_->Lookup(ino, dirent))
    return true;

  // Look in the catalogs in 2 steps: lookup inode->path, lookup path
  catalog::DirectoryEntry dirent_negative =
    catalog::DirectoryEntry(catalog::kDirentNegative);
  // Reset directory entry.  If the function returns false and dirent is no
  // the kDirentNegative, it was an I/O error
  *dirent = catalog::DirectoryEntry();

  if (nfs_maps_) {
    // NFS mode
    PathString path;
    bool retval = nfs_maps::GetPath(ino, &path);
    if (!retval) {
      *dirent = dirent_negative;
      return false;
    }
    if (catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent)) {
      // Fix inodes
      dirent->set_inode(ino);
      inode_cache_->Insert(ino, *dirent);
      return true;
    }
    return false;  // Not found in catalog or catalog load error
  }

  // Non-NFS mode
  PathString path;
  if (ino == catalog_manager_->GetRootInode()) {
    catalog_manager_->LookupPath(PathString(), catalog::kLookupSole, dirent);
    dirent->set_inode(ino);
    inode_cache_->Insert(ino, *dirent);
    return true;
  }

  bool retval = inode_tracker_->FindPath(ino, &path);
  if (!retval) {
    // Can this ever happen?
    LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForInode inode lookup failure");
    *dirent = dirent_negative;
    return false;
  }
  if (catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent)) {
    // Fix inodes
    dirent->set_inode(ino);
    inode_cache_->Insert(ino, *dirent);
    return true;
  }

  // Can happen after reload of catalogs or on catalog load failure
  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForInode path lookup failure");
  return false;
}


static bool GetDirentForPath(const PathString &path,
                             catalog::DirectoryEntry *dirent)
{
  uint64_t live_inode = 0;
  if (!nfs_maps_)
    live_inode = inode_tracker_->FindInode(path);

  shash::Md5 md5path(path.GetChars(), path.GetLength());
  if (md5path_cache_->Lookup(md5path, dirent)) {
    if (dirent->GetSpecial() == catalog::kDirentNegative)
      return false;
    if (!nfs_maps_ && (live_inode != 0))
      dirent->set_inode(live_inode);
    return true;
  }

  // Lookup inode in catalog TODO: not twice md5 calculation
  bool retval;
  retval = catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent);
  if (retval) {
    if (nfs_maps_) {
      // Fix inode
      dirent->set_inode(nfs_maps::GetInode(path));
    } else {
      if (live_inode != 0)
        dirent->set_inode(live_inode);
    }
    md5path_cache_->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  // Only insert ENOENT results into negative cache.  Otherwise it was an
  // error loading nested catalogs
  if (dirent->GetSpecial() == catalog::kDirentNegative)
    md5path_cache_->InsertNegative(md5path);
  return false;
}


static bool GetPathForInode(const fuse_ino_t ino, PathString *path) {
  // Check the path cache first
  if (path_cache_->Lookup(ino, path))
    return true;

  if (nfs_maps_) {
    // NFS mode, just a lookup
    LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - lookup in NFS maps", ino);
    if (nfs_maps::GetPath(ino, path)) {
      path_cache_->Insert(ino, *path);
      return true;
    }
    return false;
  }

  if (ino == catalog_manager_->GetRootInode())
    return true;

  LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - looking in inode tracker", ino);
  bool retval = inode_tracker_->FindPath(ino, path);
  assert(retval);
  path_cache_->Insert(ino, *path);
  return true;
}


/**
 * Find the inode number of a file name in a directory given by inode.
 * This or getattr is called as kind of prerequisit to every operation.
 * We do check catalog TTL here (and reload, if necessary).
 */
static void cvmfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
  perf::Inc(n_fs_lookup_);
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  RemountCheck();

  remount_fence_->Enter();
  parent = catalog_manager_->MangleInode(parent);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_lookup in parent inode: %"PRIu64" for name: %s",
           uint64_t(parent), name);

  PathString path;
  PathString parent_path;
  catalog::DirectoryEntry dirent;
  struct fuse_entry_param result;

  memset(&result, 0, sizeof(result));
  double timeout = GetKcacheTimeout();
  result.attr_timeout = timeout;
  result.entry_timeout = timeout;

  // Special NFS lookups: . and ..
  if ((strcmp(name, ".") == 0) || (strcmp(name, "..") == 0)) {
    if (GetDirentForInode(parent, &dirent)) {
      if (strcmp(name, ".") == 0) {
        goto lookup_reply_positive;
      } else {
        // Lookup for ".."
        if (dirent.inode() == catalog_manager_->GetRootInode()) {
          dirent.set_inode(1);
          goto lookup_reply_positive;
        }
        if (!GetPathForInode(parent, &parent_path))
          goto lookup_reply_negative;
        if (GetDirentForPath(GetParentPath(parent_path), &dirent))
          goto lookup_reply_positive;
      }
    }
    // No entry for "." or no entry for ".."
    if (dirent.GetSpecial() == catalog::kDirentNegative)
      goto lookup_reply_negative;
    else
      goto lookup_reply_error;
    assert(false);
  }

  if (!GetPathForInode(parent, &parent_path)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "no path for parent inode found");
    goto lookup_reply_negative;
  }

  path.Assign(parent_path);
  path.Append("/", 1);
  path.Append(name, strlen(name));
  tracer::Trace(tracer::kFuseLookup, path, "lookup()");
  if (!GetDirentForPath(path, &dirent)) {
    if (dirent.GetSpecial() == catalog::kDirentNegative)
      goto lookup_reply_negative;
    else
      goto lookup_reply_error;
  }

 lookup_reply_positive:
  if (!nfs_maps_)
    inode_tracker_->VfsGet(dirent.inode(), path);
  remount_fence_->Leave();
  result.ino = dirent.inode();
  result.attr = dirent.GetStatStructure();
  fuse_reply_entry(req, &result);
  return;

 lookup_reply_negative:
  remount_fence_->Leave();
  perf::Inc(n_fs_lookup_negative_);
  result.ino = 0;
  fuse_reply_entry(req, &result);
  return;

 lookup_reply_error:
  remount_fence_->Leave();
  fuse_reply_err(req, EIO);
}


/**
 *
 */
static void cvmfs_forget(
  fuse_req_t req,
  fuse_ino_t ino,
  unsigned long nlookup  // NOLINT
) {
  perf::Inc(cvmfs::n_fs_forget_);

  // The libfuse high-level library does the same
  if (ino == FUSE_ROOT_ID) {
    fuse_reply_none(req);
    return;
  }

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "forget on inode %"PRIu64" by %u",
           uint64_t(ino), nlookup);
  if (!nfs_maps_)
    inode_tracker_->VfsPut(ino, nlookup);
  remount_fence_->Leave();
  fuse_reply_none(req);
}


/**
 * Looks into dirent to decide if this is an EIO negative reply or an
 * ENOENT negative reply
 */
static void ReplyNegative(const catalog::DirectoryEntry &dirent,
                          fuse_req_t req)
{
  if (dirent.GetSpecial() == catalog::kDirentNegative)
    fuse_reply_err(req, ENOENT);
  else
    fuse_reply_err(req, EIO);
}


/**
 * Transform a cvmfs dirent into a struct stat.
 */
static void cvmfs_getattr(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
  perf::Inc(n_fs_stat_);
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  RemountCheck();

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_getattr (stat) for inode: %"PRIu64,
           uint64_t(ino));

  if (!CheckVoms(*fuse_ctx)) {
    remount_fence_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);
  remount_fence_->Leave();

  if (!found) {
    ReplyNegative(dirent, req);
    return;
  }

  struct stat info = dirent.GetStatStructure();

  fuse_reply_attr(req, &info, GetKcacheTimeout());
}


/**
 * Reads a symlink from the catalog.  Environment variables are expanded.
 */
static void cvmfs_readlink(fuse_req_t req, fuse_ino_t ino) {
  perf::Inc(n_fs_readlink_);
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_readlink on inode: %"PRIu64,
           uint64_t(ino));

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);
  remount_fence_->Leave();

  if (!found) {
    ReplyNegative(dirent, req);
    return;
  }

  if (!dirent.IsLink()) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  fuse_reply_readlink(req, dirent.symlink().c_str());
}


static void AddToDirListing(const fuse_req_t req,
                            const char *name, const struct stat *stat_info,
                            BigVector<char> *listing)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "Add to listing: %s, inode %"PRIu64,
           name, uint64_t(stat_info->st_ino));
  size_t remaining_size = listing->capacity() - listing->size();
  const size_t entry_size = fuse_add_direntry(req, NULL, 0, name, stat_info, 0);

  while (entry_size > remaining_size) {
    listing->DoubleCapacity();
    remaining_size = listing->capacity() - listing->size();
  }

  char *buffer;
  bool large_alloc;
  listing->ShareBuffer(&buffer, &large_alloc);
  fuse_add_direntry(req, buffer + listing->size(),
                    remaining_size, name, stat_info,
                    listing->size() + entry_size);
  listing->SetSize(listing->size() + entry_size);
}


/**
 * Open a directory for listing.
 */
static void cvmfs_opendir(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  RemountCheck();

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %"PRIu64,
           uint64_t(ino));

  if (!CheckVoms(*fuse_ctx)) {
    remount_fence_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  PathString path;
  catalog::DirectoryEntry d;
  bool found = GetPathForInode(ino, &path);
  if (!found) {
    remount_fence_->Leave();
    fuse_reply_err(req, ENOENT);
    return;
  }
  found = GetDirentForInode(ino, &d);

  if (!found) {
    remount_fence_->Leave();
    ReplyNegative(d, req);
    return;
  }
  if (!d.IsDirectory()) {
    remount_fence_->Leave();
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %"PRIu64", path %s",
           uint64_t(ino), path.c_str());

  // Build listing
  BigVector<char> fuse_listing(512);

  // Add current directory link
  struct stat info;
  info = d.GetStatStructure();
  AddToDirListing(req, ".", &info, &fuse_listing);

  // Add parent directory link
  catalog::DirectoryEntry p;
  if (d.inode() != catalog_manager_->GetRootInode() &&
      GetDirentForPath(GetParentPath(path), &p))
  {
    info = p.GetStatStructure();
    AddToDirListing(req, "..", &info, &fuse_listing);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  bool retval = catalog_manager_->ListingStat(path, &listing_from_catalog);

  if (!retval) {
    remount_fence_->Leave();
    fuse_listing.Clear();  // Buffer is shared, empty manually
    fuse_reply_err(req, EIO);
    return;
  }
  for (unsigned i = 0; i < listing_from_catalog.size(); ++i) {
    // Fix inodes
    PathString entry_path;
    entry_path.Assign(path);
    entry_path.Append("/", 1);
    entry_path.Append(listing_from_catalog.AtPtr(i)->name.GetChars(),
                      listing_from_catalog.AtPtr(i)->name.GetLength());

    catalog::DirectoryEntry entry_dirent;
    if (!GetDirentForPath(entry_path, &entry_dirent)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "listing entry %s vanished, skipping",
               entry_path.c_str());
      continue;
    }

    struct stat fixed_info = listing_from_catalog.AtPtr(i)->info;
    fixed_info.st_ino = entry_dirent.inode();
    AddToDirListing(req, listing_from_catalog.AtPtr(i)->name.c_str(),
                    &fixed_info, &fuse_listing);
  }
  remount_fence_->Leave();

  DirectoryListing stream_listing;
  stream_listing.size = fuse_listing.size();
  stream_listing.capacity = fuse_listing.capacity();
  bool large_alloc;
  fuse_listing.ShareBuffer(&stream_listing.buffer, &large_alloc);
  if (large_alloc)
    stream_listing.capacity = 0;

  // Save the directory listing and return a handle to the listing
  pthread_mutex_lock(&lock_directory_handles_);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "linking directory handle %d to dir inode: %"PRIu64,
           next_directory_handle_, uint64_t(ino));
  (*directory_handles_)[next_directory_handle_] = stream_listing;
  fi->fh = next_directory_handle_;
  ++next_directory_handle_;
  pthread_mutex_unlock(&lock_directory_handles_);
  perf::Inc(n_fs_dir_open_);
  perf::Inc(no_open_dirs_);

  fuse_reply_open(req, fi);
}


/**
 * Release a directory.
 */
static void cvmfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi)
{
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_releasedir on inode %"PRIu64
           ", handle %d", uint64_t(ino), fi->fh);

  int reply = 0;

  pthread_mutex_lock(&lock_directory_handles_);
  DirectoryHandles::iterator iter_handle =
    directory_handles_->find(fi->fh);
  if (iter_handle != directory_handles_->end()) {
    if (iter_handle->second.capacity == 0)
      smunmap(iter_handle->second.buffer);
    else
      free(iter_handle->second.buffer);
    directory_handles_->erase(iter_handle);
    pthread_mutex_unlock(&lock_directory_handles_);
    perf::Dec(no_open_dirs_);
  } else {
    pthread_mutex_unlock(&lock_directory_handles_);
    reply = EINVAL;
  }

  fuse_reply_err(req, reply);
}


/**
 * Very large directory listings have to be sent in slices.
 */
static void ReplyBufferSlice(const fuse_req_t req, const char *buffer,
                             const size_t buffer_size, const off_t offset,
                             const size_t max_size)
{
  if (offset < static_cast<int>(buffer_size)) {
    fuse_reply_buf(req, buffer + offset,
      std::min(static_cast<size_t>(buffer_size - offset), max_size));
  } else {
    fuse_reply_buf(req, NULL, 0);
  }
}


/**
 * Read the directory listing.
 */
static void cvmfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                          off_t off, struct fuse_file_info *fi)
{
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_readdir on inode %"PRIu64" reading %d bytes from offset %d",
           uint64_t(catalog_manager_->MangleInode(ino)), size, off);

  DirectoryListing listing;

  pthread_mutex_lock(&lock_directory_handles_);
  DirectoryHandles::const_iterator iter_handle =
    directory_handles_->find(fi->fh);
  if (iter_handle != directory_handles_->end()) {
    listing = iter_handle->second;
    pthread_mutex_unlock(&lock_directory_handles_);

    ReplyBufferSlice(req, listing.buffer, listing.size, off, size);
    return;
  }

  pthread_mutex_unlock(&lock_directory_handles_);
  fuse_reply_err(req, EINVAL);
}


/**
 * Open a file from cache.  If necessary, file is downloaded first.
 *
 * \return Read-only file descriptor in fi->fh or kChunkedFileHandle for
 * chunked files
 */
static void cvmfs_open(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on inode: %"PRIu64, uint64_t(ino));

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;

  bool found = GetPathForInode(ino, &path);
  if (!found) {
    remount_fence_->Leave();
    fuse_reply_err(req, ENOENT);
    return;
  }
  found = GetDirentForInode(ino, &dirent);
  if (!found) {
    remount_fence_->Leave();
    ReplyNegative(dirent, req);
    return;
  }

  if (!CheckVoms(*fuse_ctx)) {
    remount_fence_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  // Don't check.  Either done by the OS or one wants to purposefully work
  // around wrong open flags
  // if ((fi->flags & 3) != O_RDONLY) {
  //   fuse_reply_err(req, EROFS);
  //   return;
  // }
#ifdef __APPLE__
  if ((fi->flags & O_SHLOCK) || (fi->flags & O_EXLOCK)) {
    remount_fence_->Leave();
    fuse_reply_err(req, EOPNOTSUPP);
    return;
  }
#endif
  if (fi->flags & O_EXCL) {
    remount_fence_->Leave();
    fuse_reply_err(req, EEXIST);
    return;
  }

  perf::Inc(n_fs_open_);  // Count actual open / fetch operations

  if (!dirent.IsChunkedFile()) {
    remount_fence_->Leave();
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "chunked file %s opened (download delayed to read() call)",
             path.c_str());

    if (perf::Xadd(no_open_files_, 1) >=
        (static_cast<int>(max_open_files_))-kNumReservedFd)
    {
      perf::Dec(no_open_files_);
      remount_fence_->Leave();
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "open file descriptor limit exceeded");
      fuse_reply_err(req, EMFILE);
      return;
    }

    chunk_tables_->Lock();
    if (!chunk_tables_->inode2chunks.Contains(ino)) {
      chunk_tables_->Unlock();

      // Retrieve File chunks from the catalog
      FileChunkList *chunks = new FileChunkList();
      if (!catalog_manager_->ListFileChunks(path, dirent.hash_algorithm(),
                                           chunks) ||
          chunks->IsEmpty())
      {
        remount_fence_->Leave();
        LogCvmfs(kLogCvmfs, kLogDebug| kLogSyslogErr, "file %s is marked as "
                 "'chunked', but no chunks found.", path.c_str());
        fuse_reply_err(req, EIO);
        return;
      }
      remount_fence_->Leave();

      chunk_tables_->Lock();
      // Check again to avoid race
      if (!chunk_tables_->inode2chunks.Contains(ino)) {
        chunk_tables_->inode2chunks.Insert(
          ino, FileChunkReflist(chunks, path, dirent.compression_algorithm(),
                                dirent.IsExternalFile()));
        chunk_tables_->inode2references.Insert(ino, 1);
      } else {
        uint32_t refctr;
        bool retval = chunk_tables_->inode2references.Lookup(ino, &refctr);
        assert(retval);
        chunk_tables_->inode2references.Insert(ino, refctr+1);
      }
    } else {
      remount_fence_->Leave();
      uint32_t refctr;
      bool retval = chunk_tables_->inode2references.Lookup(ino, &refctr);
      assert(retval);
      chunk_tables_->inode2references.Insert(ino, refctr+1);
    }

    // Update the chunk handle list
    LogCvmfs(kLogCvmfs, kLogDebug,
             "linking chunk handle %d to inode: %"PRIu64,
             chunk_tables_->next_handle, uint64_t(ino));
    chunk_tables_->handle2fd.Insert(chunk_tables_->next_handle, ChunkFd());
    fi->fh = static_cast<uint64_t>(-chunk_tables_->next_handle);
    ++chunk_tables_->next_handle;
    chunk_tables_->Unlock();

    fuse_reply_open(req, fi);
    return;
  }

  fd = (dirent.IsExternalFile() ? external_fetcher_ : fetcher_)->Fetch(
    dirent.checksum(),
    dirent.size(),
    string(path.GetChars(), path.GetLength()),
    dirent.compression_algorithm(),
    volatile_repository_ ? cache::CacheManager::kTypeVolatile :
                           cache::CacheManager::kTypeRegular);

  if (fd >= 0) {
    if (perf::Xadd(no_open_files_, 1) <
        (static_cast<int>(max_open_files_))-kNumReservedFd) {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
      /*fi->keep_cache = kcache_timeout_ == 0.0 ? 0 : 1;
      if (dirent.cached_mtime() != dirent.mtime()) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "file might be new or changed, invalidating cache (%d %d "
                 "%"PRIu64")", dirent.mtime(), dirent.cached_mtime(), uint64_t(ino));
        fi->keep_cache = 0;
        dirent.set_cached_mtime(dirent.mtime());
        inode_cache_->Insert(ino, dirent);
      }*/
      fi->keep_cache = 0;
      fi->fh = fd;
      fuse_reply_open(req, fi);
      return;
    } else {
      if (cache_manager_->Close(fd) == 0) perf::Dec(no_open_files_);
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "open file descriptor limit exceeded");
      fuse_reply_err(req, EMFILE);
      return;
    }
    assert(false);
  }

  // fd < 0
  LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
           "failed to open inode: %"PRIu64", CAS key %s, error code %d",
           uint64_t(ino), dirent.checksum().ToString().c_str(), errno);
  if (errno == EMFILE) {
    fuse_reply_err(req, EMFILE);
    return;
  }

  backoff_throttle_->Throttle();

  perf::Inc(n_io_error_);
  fuse_reply_err(req, -fd);
}


/**
 * Redirected to pread into cache.
 */
static void cvmfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_read inode: %"PRIu64" reading %d bytes from offset %d fd %d",
           uint64_t(catalog_manager_->MangleInode(ino)), size, off, fi->fh);
  perf::Inc(n_fs_read_);

  // Get data chunk (<=128k guaranteed by Fuse)
  char *data = static_cast<char *>(alloca(size));
  unsigned int overall_bytes_fetched = 0;

  // Do we have a a chunked file?
  if (static_cast<int64_t>(fi->fh) < 0) {
    const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
    ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

    const uint64_t chunk_handle =
      static_cast<uint64_t>(-static_cast<int64_t>(fi->fh));
    ChunkFd chunk_fd;
    FileChunkReflist chunks;
    bool retval;

    // Fetch chunk list and file descriptor
    chunk_tables_->Lock();
    retval = chunk_tables_->inode2chunks.Lookup(ino, &chunks);
    assert(retval);
    chunk_tables_->Unlock();

    unsigned chunk_idx = chunks.FindChunkIdx(off);

    // Lock chunk handle
    pthread_mutex_t *handle_lock = chunk_tables_->Handle2Lock(chunk_handle);
    LockMutex(handle_lock);
    chunk_tables_->Lock();
    retval = chunk_tables_->handle2fd.Lookup(chunk_handle, &chunk_fd);
    assert(retval);
    chunk_tables_->Unlock();

    // Fetch all needed chunks and read the requested data
    off_t offset_in_chunk = off - chunks.list->AtPtr(chunk_idx)->offset();
    do {
      // Open file descriptor to chunk
      if ((chunk_fd.fd == -1) || (chunk_fd.chunk_idx != chunk_idx)) {
        if (chunk_fd.fd != -1) cache_manager_->Close(chunk_fd.fd);
        string verbose_path = "Part of " + chunks.path.ToString();
        if (chunks.external_data) {
          chunk_fd.fd = external_fetcher_->Fetch(
            chunks.list->AtPtr(chunk_idx)->content_hash(),
            chunks.list->AtPtr(chunk_idx)->size(),
            verbose_path,
            chunks.compression_alg,
            volatile_repository_ ? cache::CacheManager::kTypeVolatile
                                 : cache::CacheManager::kTypeRegular,
            chunks.path.ToString(),
            chunks.list->AtPtr(chunk_idx)->offset());
        } else {
          chunk_fd.fd = fetcher_->Fetch(
            chunks.list->AtPtr(chunk_idx)->content_hash(),
            chunks.list->AtPtr(chunk_idx)->size(),
            verbose_path,
            chunks.compression_alg,
            volatile_repository_ ? cache::CacheManager::kTypeVolatile
                                 : cache::CacheManager::kTypeRegular);
        }
        if (chunk_fd.fd < 0) {
          chunk_fd.fd = -1;
          chunk_tables_->Lock();
          chunk_tables_->handle2fd.Insert(chunk_handle, chunk_fd);
          chunk_tables_->Unlock();
          UnlockMutex(handle_lock);
          fuse_reply_err(req, EIO);
          return;
        }
        chunk_fd.chunk_idx = chunk_idx;
      }

      LogCvmfs(kLogCvmfs, kLogDebug, "reading from chunk fd %d",
               chunk_fd.fd);
      // Read data from chunk
      const size_t bytes_to_read = size - overall_bytes_fetched;
      const size_t remaining_bytes_in_chunk =
        chunks.list->AtPtr(chunk_idx)->size() - offset_in_chunk;
      size_t bytes_to_read_in_chunk =
        std::min(bytes_to_read, remaining_bytes_in_chunk);
      const int64_t bytes_fetched = cache_manager_->Pread(
        chunk_fd.fd,
        data + overall_bytes_fetched,
        bytes_to_read_in_chunk,
        offset_in_chunk);

      if (bytes_fetched < 0) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr, "read err no %"PRId64" (%s)",
                 bytes_fetched, chunks.path.ToString().c_str());
        chunk_tables_->Lock();
        chunk_tables_->handle2fd.Insert(chunk_handle, chunk_fd);
        chunk_tables_->Unlock();
        UnlockMutex(handle_lock);
        fuse_reply_err(req, -bytes_fetched);
        return;
      }
      overall_bytes_fetched += bytes_fetched;

      // Proceed to the next chunk to keep on reading data
      ++chunk_idx;
      offset_in_chunk = 0;
    } while ((overall_bytes_fetched < size) &&
             (chunk_idx < chunks.list->size()));

    // Update chunk file descriptor
    chunk_tables_->Lock();
    chunk_tables_->handle2fd.Insert(chunk_handle, chunk_fd);
    chunk_tables_->Unlock();
    UnlockMutex(handle_lock);
    LogCvmfs(kLogCvmfs, kLogDebug, "released chunk file descriptor %d",
             chunk_fd.fd);
  } else {
    const int64_t fd = fi->fh;
    int64_t nbytes = cache_manager_->Pread(fd, data, size, off);
    if (nbytes < 0) {
      fuse_reply_err(req, -nbytes);
      return;
    }
    overall_bytes_fetched = nbytes;
  }

  // Push it to user
  fuse_reply_buf(req, data, overall_bytes_fetched);
  LogCvmfs(kLogCvmfs, kLogDebug, "pushed %d bytes to user",
           overall_bytes_fetched);
}


/**
 * File close operation, redirected into cache.
 */
static void cvmfs_release(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_release on inode: %"PRIu64,
           uint64_t(ino));
  const int64_t fd = fi->fh;

  // do we have a chunked file?
  if (static_cast<int64_t>(fi->fh) < 0) {
    const uint64_t chunk_handle =
      static_cast<uint64_t>(-static_cast<int64_t>(fi->fh));
    LogCvmfs(kLogCvmfs, kLogDebug, "releasing chunk handle %"PRIu64,
             chunk_handle);
    ChunkFd chunk_fd;
    FileChunkReflist chunks;
    uint32_t refctr;
    bool retval;

    chunk_tables_->Lock();
    retval = chunk_tables_->handle2fd.Lookup(chunk_handle, &chunk_fd);
    assert(retval);
    chunk_tables_->handle2fd.Erase(chunk_handle);

    retval = chunk_tables_->inode2references.Lookup(ino, &refctr);
    assert(retval);
    refctr--;
    if (refctr == 0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "releasing chunk list for inode %"PRIu64,
               uint64_t(ino));
      FileChunkReflist to_delete;
      retval = chunk_tables_->inode2chunks.Lookup(ino, &to_delete);
      assert(retval);
      chunk_tables_->inode2references.Erase(ino);
      chunk_tables_->inode2chunks.Erase(ino);
      delete to_delete.list;
    } else {
      chunk_tables_->inode2references.Insert(ino, refctr);
    }
    chunk_tables_->Unlock();

    if (chunk_fd.fd != -1)
      cache_manager_->Close(chunk_fd.fd);
    perf::Dec(no_open_files_);
  } else {
    if (cache_manager_->Close(fd) == 0) {
      perf::Dec(no_open_files_);
    }
  }
  fuse_reply_err(req, 0);
}


static void cvmfs_statfs(fuse_req_t req, fuse_ino_t ino) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_statfs on inode: %"PRIu64,
           uint64_t(ino));

  // If we return 0 it will cause the fs to be ignored in "df"
  struct statvfs info;
  memset(&info, 0, sizeof(info));

  // Unmanaged cache
  if (!cache_manager_->quota_mgr()->IsEnforcing()) {
    fuse_reply_statfs(req, &info);
    return;
  }

  uint64_t available = 0;
  uint64_t size = cache_manager_->quota_mgr()->GetSize();
  info.f_bsize = 1;

  if (cache_manager_->quota_mgr()->GetCapacity() == (uint64_t)(-1)) {
    // Unrestricted cache, look at free space on cache dir fs
    struct statfs cache_buf;
    if (statfs(".", &cache_buf) == 0) {
      available = cache_buf.f_bavail * cache_buf.f_bsize;
      info.f_blocks = size + available;
    } else {
      info.f_blocks = size;
    }
  } else {
    // Take values from LRU module
    info.f_blocks = cache_manager_->quota_mgr()->GetCapacity();
    available = cache_manager_->quota_mgr()->GetCapacity() - size;
  }

  info.f_bfree = info.f_bavail = available;

  // Inodes / entries
  remount_fence_->Enter();
  info.f_files = catalog_manager_->all_inodes();
  info.f_ffree = info.f_favail =
    catalog_manager_->all_inodes() - catalog_manager_->loaded_inodes();
  remount_fence_->Leave();

  fuse_reply_statfs(req, &info);
}


#ifdef __APPLE__
static void cvmfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                           size_t size, uint32_t position)
#else
static void cvmfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                           size_t size)
#endif
{
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_getxattr on inode: %"PRIu64" for xattr: %s",
           uint64_t(ino), name);

  if (!CheckVoms(*fuse_ctx)) {
    remount_fence_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  const string attr = name;
  catalog::DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);
  bool retval;
  XattrList xattrs;

  if (d.HasXattrs()) {
    PathString path;
    retval = GetPathForInode(ino, &path);
    assert(retval);
    retval = catalog_manager_->LookupXattrs(path, &xattrs);
    assert(retval);
  }
  if (d.IsLink()) {
    PathString path;
    catalog::LookupOptions lookup_options = static_cast<catalog::LookupOptions>(
      catalog::kLookupSole | catalog::kLookupRawSymlink);
    catalog::DirectoryEntry raw_symlink;
    retval = catalog_manager_->LookupPath(path, lookup_options, &raw_symlink);
    assert(retval);
    d.set_symlink(raw_symlink.symlink());
  }
  remount_fence_->Leave();

  if (!found) {
    ReplyNegative(d, req);
    return;
  }

  string attribute_value;
  std::string lookup_value;

  if (attr == "user.pid") {
    attribute_value = StringifyInt(pid_);
  } else if (attr == "user.version") {
    attribute_value = string(VERSION) + "." + string(CVMFS_PATCH_LEVEL);
  } else if (attr == "user.pubkeys") {
    attribute_value = cvmfs::signature_manager_->GetActivePubkeys();
  } else if (attr == "user.hash") {
    if (!d.checksum().IsNull()) {
      attribute_value = d.checksum().ToString();
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.lhash") {
    if (!d.checksum().IsNull()) {
      string result;
      int fd = cache_manager_->Open(d.checksum());
      if (fd < 0) {
        attribute_value = "Not in cache";
      } else {
        shash::Any hash(d.checksum().algorithm);
        int retval_i = cache_manager_->ChecksumFd(fd, &hash);
        if (retval_i != 0)
          attribute_value = "I/O error (" + StringifyInt(retval_i) + ")";
        else
          attribute_value = hash.ToString();
        cache_manager_->Close(fd);
      }
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if ((attr == "xfsroot.rawlink") || (attr == "user.rawlink")) {
    if (d.IsLink()) {
      attribute_value = d.symlink().ToString();
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.revision") {
    const uint64_t revision = catalog_manager_->GetRevision();
    attribute_value = StringifyInt(revision);
  } else if (attr == "user.root_hash") {
    attribute_value = catalog_manager_->GetRootHash().ToString();
  } else if ((attr == "user.voms_authz") &&
             catalog_manager_->GetVOMSAuthz(&lookup_value))
  {
    attribute_value = lookup_value;
  } else if (attr == "user.tag") {
    attribute_value = *repository_tag_;
  } else if (attr == "user.expires") {
    if (catalogs_valid_until_ == kIndefiniteDeadline) {
      attribute_value = "never (fixed root catalog)";
    } else {
      time_t now = time(NULL);
      attribute_value = StringifyInt((catalogs_valid_until_-now)/60);
    }
  } else if (attr == "user.maxfd") {
    attribute_value = StringifyInt(max_open_files_ - kNumReservedFd);
  } else if (attr == "user.usedfd") {
    attribute_value = no_open_files_->ToString();
  } else if (attr == "user.useddirp") {
    attribute_value = no_open_dirs_->ToString();
  } else if (attr == "user.nioerr") {
    attribute_value = n_io_error_->ToString();
  } else if (attr == "user.proxy") {
    vector< vector<download::DownloadManager::ProxyInfo> > proxy_chain;
    unsigned current_group;
    download_manager_->GetProxyInfo(&proxy_chain, &current_group, NULL);
    if (proxy_chain.size()) {
      attribute_value = proxy_chain[current_group][0].url;
    } else {
      attribute_value = "DIRECT";
    }
  } else if (attr == "user.authz") {
    bool has_authz = catalog_manager_->GetVOMSAuthz(&attribute_value);
    if (!has_authz) {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.chunks") {
    if (d.IsRegular()) {
      if (d.IsChunkedFile()) {
        PathString path;
        retval = GetPathForInode(ino, &path);
        assert(retval);

        FileChunkList chunks;
        if (!catalog_manager_->ListFileChunks(path, d.hash_algorithm(),
                                              &chunks) ||
            chunks.IsEmpty())
        {
          LogCvmfs(kLogCvmfs, kLogDebug| kLogSyslogErr, "file %s is marked as "
                   "'chunked', but no chunks found.", path.c_str());
          fuse_reply_err(req, EIO);
          return;
        } else {
          attribute_value = StringifyInt(chunks.size());
        }
      } else {
        attribute_value = "1";
      }
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.external_file") {
    if (d.IsRegular()) {
      attribute_value = d.IsExternalFile() ? "1" : "0";
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.external_host") {
    vector<string> host_chain;
    vector<int> rtt;
    unsigned current_host;
    external_download_manager_->GetHostInfo(&host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      attribute_value = string(host_chain[current_host]);
    } else {
      attribute_value = "internal error: no hosts defined";
    }
  } else if (attr == "user.compression") {
    if (d.IsRegular()) {
      attribute_value = zlib::AlgorithmName(d.compression_algorithm());
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.host") {
    vector<string> host_chain;
    vector<int> rtt;
    unsigned current_host;
    download_manager_->GetHostInfo(&host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      attribute_value = string(host_chain[current_host]);
    } else {
      attribute_value = "internal error: no hosts defined";
    }
  } else if (attr == "user.host_list") {
    vector<string> host_chain;
    vector<int> rtt;
    unsigned current_host;
    download_manager_->GetHostInfo(&host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      attribute_value = host_chain[current_host];
      for (unsigned i = 1; i < host_chain.size(); ++i) {
        attribute_value +=
          ";" + host_chain[(i+current_host) % host_chain.size()];
      }
    } else {
      attribute_value = "internal error: no hosts defined";
    }
  } else if (attr == "user.uptime") {
    time_t now = time(NULL);
    uint64_t uptime = now - boot_time_;
    attribute_value = StringifyInt(uptime / 60);
  } else if (attr == "user.nclg") {
    const int num_catalogs = catalog_manager_->GetNumCatalogs();
    attribute_value = StringifyInt(num_catalogs);
  } else if (attr == "user.nopen") {
    attribute_value = n_fs_open_->ToString();
  } else if (attr == "user.ndiropen") {
    attribute_value = n_fs_dir_open_->ToString();
  } else if (attr == "user.ndownload") {
    attribute_value = statistics_->Lookup("fetch.n_downloads")->Print();
  } else if (attr == "user.timeout") {
    unsigned seconds, seconds_direct;
    download_manager_->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds);
  } else if (attr == "user.timeout_direct") {
    unsigned seconds, seconds_direct;
    download_manager_->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.external_timeout") {
    unsigned seconds, seconds_direct;
    download_manager_->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.rx") {
    int64_t rx =
      cvmfs::statistics_->Lookup("download.sz_transferred_bytes")->Get();
    attribute_value = StringifyInt(rx/1024);
  } else if (attr == "user.speed") {
    int64_t rx =
      cvmfs::statistics_->Lookup("download.sz_transferred_bytes")->Get();
    int64_t time =
      cvmfs::statistics_->Lookup("download.sz_transfer_time")->Get();
    if (time == 0)
      attribute_value = "n/a";
    else
      attribute_value = StringifyInt((rx/1024)/time);
  } else if (attr == "user.fqrn") {
    attribute_value = *repository_name_;
  } else if (attr == "user.inode_max") {
    attribute_value = StringifyInt(
      inode_generation_info_.inode_generation +
      catalog_manager_->inode_gauge());
  } else {
    if (!xattrs.Get(attr, &attribute_value)) {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  }

  if (size == 0) {
    fuse_reply_xattr(req, attribute_value.length());
  } else if (size >= attribute_value.length()) {
    fuse_reply_buf(req, &attribute_value[0], attribute_value.length());
  } else {
    fuse_reply_err(req, ERANGE);
  }
}


static void cvmfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

  remount_fence_->Enter();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_listxattr on inode: %"PRIu64", size %u [hide xattrs %d]",
           uint64_t(ino), size, hide_magic_xattrs_);

  catalog::DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);
  XattrList xattrs;
  if (d.HasXattrs()) {
    PathString path;
    bool retval = GetPathForInode(ino, &path);
    assert(retval);
    retval = catalog_manager_->LookupXattrs(path, &xattrs);
    assert(retval);
  }
  remount_fence_->Leave();

  if (!found) {
    ReplyNegative(d, req);
    return;
  }

  const char base_list[] = "user.pid\0user.version\0user.revision\0"
    "user.root_hash\0user.expires\0user.maxfd\0user.usedfd\0user.nioerr\0"
    "user.host\0user.proxy\0user.uptime\0user.nclg\0user.nopen\0"
    "user.ndownload\0user.timeout\0user.timeout_direct\0user.rx\0user.speed\0"
    "user.fqrn\0user.ndiropen\0user.inode_max\0user.tag\0user.host_list\0"
    "user.external_host\0user.external_timeout\0user.pubkeys\0";
  string attribute_list;
  if (hide_magic_xattrs_) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Hiding extended attributes");
    attribute_list = xattrs.ListKeysPosix("");
  } else {
    attribute_list = string(base_list, sizeof(base_list)-1);
    if (!d.checksum().IsNull()) {
      const char regular_file_list[] = "user.hash\0user.lhash\0";
      attribute_list += string(regular_file_list, sizeof(regular_file_list)-1);
    }

    if (d.IsLink()) {
      const char symlink_list[] = "xfsroot.rawlink\0user.rawlink\0";
      attribute_list += string(symlink_list, sizeof(symlink_list)-1);
    } else if (d.IsRegular()) {
      const char regular_file_list[] = "user.external_file\0user.compression\0"
                                       "user.chunks\0";
      attribute_list += string(regular_file_list, sizeof(regular_file_list)-1);
    }

    if (catalog_manager_->GetVOMSAuthz(NULL)) {
      attribute_list += "user.authz\0";
    }
    attribute_list = xattrs.ListKeysPosix(attribute_list);
  }

  if (size == 0) {
    fuse_reply_xattr(req, attribute_list.length());
  } else if (size >= attribute_list.length()) {
    if (attribute_list.empty())
      fuse_reply_buf(req, NULL, 0);
    else
      fuse_reply_buf(req, &attribute_list[0], attribute_list.length());
  } else {
    fuse_reply_err(req, ERANGE);
  }
}


bool Evict(const string &path) {
  catalog::DirectoryEntry dirent;
  remount_fence_->Enter();
  const bool found = GetDirentForPath(PathString(path), &dirent);
  remount_fence_->Leave();

  if (!found || !dirent.IsRegular())
    return false;
  cache_manager_->quota_mgr()->Remove(dirent.checksum());
  return true;
}


bool Pin(const string &path) {
  catalog::DirectoryEntry dirent;
  remount_fence_->Enter();
  const bool found = GetDirentForPath(PathString(path), &dirent);
  if (!found || !dirent.IsRegular()) {
    remount_fence_->Leave();
    return false;
  }

  if (!dirent.IsChunkedFile()) {
    remount_fence_->Leave();
  } else {
    FileChunkList chunks;
    catalog_manager_->ListFileChunks(PathString(path), dirent.hash_algorithm(),
                                     &chunks);
    remount_fence_->Leave();
    for (unsigned i = 0; i < chunks.size(); ++i) {
      bool retval =
        cache_manager_->quota_mgr()->Pin(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          false);
      if (!retval)
        return false;
      int fd = -1;
      if (dirent.IsExternalFile()) {
        fd = external_fetcher_->Fetch(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          dirent.compression_algorithm(),
          cache::CacheManager::kTypePinned,
          path,
          chunks.AtPtr(i)->offset());
      } else {
        fd = fetcher_->Fetch(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          dirent.compression_algorithm(),
          cache::CacheManager::kTypePinned);
      }
      if (fd < 0) {
        return false;
      }
      cache_manager_->Close(fd);
    }
    return true;
  }

  bool retval = cache_manager_->quota_mgr()->Pin(
    dirent.checksum(), dirent.size(), path, false);
  if (!retval)
    return false;
  int fd = (dirent.IsExternalFile() ? external_fetcher_ : fetcher_)->Fetch(
    dirent.checksum(), dirent.size(), path, dirent.compression_algorithm(),
    cache::CacheManager::kTypePinned);
  if (fd < 0) {
    return false;
  }
  cache_manager_->Close(fd);
  return true;
}


/**
 * Do after-daemon() initialization
 */
static void cvmfs_init(void *userdata, struct fuse_conn_info *conn) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_init");

  // NFS support
#ifdef CVMFS_NFS_SUPPORT
  conn->want |= FUSE_CAP_EXPORT_SUPPORT;
#endif
}

static void cvmfs_destroy(void *unused __attribute__((unused))) {
  // The debug log is already closed at this point
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_destroy");
}

/**
 * Puts the callback functions in one single structure
 */
static void SetCvmfsOperations(struct fuse_lowlevel_ops *cvmfs_operations) {
  memset(cvmfs_operations, 0, sizeof(*cvmfs_operations));

  // Init/Fini
  cvmfs_operations->init     = cvmfs_init;
  cvmfs_operations->destroy  = cvmfs_destroy;

  cvmfs_operations->lookup      = cvmfs_lookup;
  cvmfs_operations->getattr     = cvmfs_getattr;
  cvmfs_operations->readlink    = cvmfs_readlink;
  cvmfs_operations->open        = cvmfs_open;
  cvmfs_operations->read        = cvmfs_read;
  cvmfs_operations->release     = cvmfs_release;
  cvmfs_operations->opendir     = cvmfs_opendir;
  cvmfs_operations->readdir     = cvmfs_readdir;
  cvmfs_operations->releasedir  = cvmfs_releasedir;
  cvmfs_operations->statfs      = cvmfs_statfs;
  cvmfs_operations->getxattr    = cvmfs_getxattr;
  cvmfs_operations->listxattr   = cvmfs_listxattr;
  cvmfs_operations->forget      = cvmfs_forget;
}

void UnregisterQuotaListener() {
  if (cvmfs::unpin_listener_) {
    quota::UnregisterListener(cvmfs::unpin_listener_);
    cvmfs::unpin_listener_ = NULL;
  }
  if (cvmfs::watchdog_listener_) {
    quota::UnregisterListener(cvmfs::watchdog_listener_);
    cvmfs::watchdog_listener_ = NULL;
  }
}

}  // namespace cvmfs


bool g_options_ready = false;
bool g_download_ready = false;
bool g_external_download_ready = false;
bool g_nfs_maps_ready = false;
bool g_monitor_ready = false;
bool g_signature_ready = false;
bool g_quota_ready = false;
bool g_talk_ready = false;
bool g_running_created = false;

int g_fd_lockfile = -1;
void *g_sqlite_scratch = NULL;
void *g_sqlite_page_cache = NULL;
string *g_boot_error = NULL;

__attribute__((visibility("default")))
loader::CvmfsExports *g_cvmfs_exports = NULL;


static std::string CalculateHostString(
    const std::string &fqrn,
    const std::string &parameter) {
  std::string host_str = parameter;
  vector<string> tokens = SplitString(fqrn, '.');
  const string org = tokens[0];
  host_str = ReplaceAll(host_str, "@org@", org);
  host_str = ReplaceAll(host_str, "@fqrn@", fqrn);
  return host_str;
}


static void LogSqliteError(void *user_data __attribute__((unused)),
                           int sqlite_extended_error, const char *message)
{
  int log_dest = kLogDebug;
  int sqlite_error = sqlite_extended_error & 0xFF;
  switch (sqlite_error) {
    case SQLITE_INTERNAL:
    case SQLITE_PERM:
    case SQLITE_NOMEM:
    case SQLITE_IOERR:
    case SQLITE_CORRUPT:
    case SQLITE_FULL:
    case SQLITE_CANTOPEN:
    case SQLITE_MISUSE:
    case SQLITE_FORMAT:
    case SQLITE_NOTADB:
      log_dest |= kLogSyslogErr;
      break;
    case SQLITE_WARNING:
    case SQLITE_NOTICE:
    default:
      break;
  }
  LogCvmfs(kLogCvmfs, log_dest, "SQlite3: %s (%d)",
           message, sqlite_extended_error);
}


static int Init(const loader::LoaderExports *loader_exports) {
  int retval;
  g_boot_error = new string("unknown error");
  cvmfs::loader_exports_ = loader_exports;

  uint64_t mem_cache_size = cvmfs::kDefaultMemcache;
  unsigned timeout = cvmfs::kDefaultTimeout;
  unsigned timeout_direct = cvmfs::kDefaultTimeout;
  unsigned external_timeout = cvmfs::kDefaultTimeout;
  unsigned external_timeout_direct = cvmfs::kDefaultTimeout;
  unsigned low_speed_limit = cvmfs::kDefaultLowSpeedLimit;
  unsigned dns_timeout_ms = download::DownloadManager::kDnsDefaultTimeoutMs;
  unsigned dns_retries = download::DownloadManager::kDnsDefaultRetries;
  unsigned proxy_reset_after = 0;
  unsigned host_reset_after = 0;
  unsigned max_retries = 1;
  unsigned backoff_init = 2000;
  unsigned backoff_max = 10000;
  bool send_info_header = false;
  unsigned max_ipaddr_per_proxy = 0;
  string tracefile = "";
  string cachedir = string(cvmfs::kDefaultCachedir);
  unsigned max_ttl = 0;
  int kcache_timeout = 0;
  bool rebuild_cachedb = false;
  bool nfs_source = false;
  bool nfs_shared = false;
  string nfs_shared_dir = string(cvmfs::kDefaultCachedir);
  bool shared_cache = false;
  int64_t quota_limit = cvmfs::kDefaultCacheSizeMb;
  string hostname = "";
  string proxies = "";
  string fallback_proxies = "";
  string external_proxies = "";
  string fallback_external_proxies = "";
  string dns_server = "";
  std::string external_host;
  unsigned ipfamily_prefer = 0;
  string public_keys = "";
  string root_hash = "";
  bool alt_root_path = false;
  string repository_tag = "";
  string repository_date = "";
  string alien_cache = ".";  // default: exclusive cache
  bool server_cache_mode = false;  // currently means: no rename in the cache
  string trusted_certs = "";
  string proxy_template = "";
  catalog::OwnerMap uid_map;
  catalog::OwnerMap gid_map;
  uint64_t initial_generation = 0;
  cvmfs::Uuid *uuid;
  bool use_geo_api = false;
  bool follow_redirects = false;

  cvmfs::boot_time_ = loader_exports->boot_time;
  cvmfs::backoff_throttle_ = new BackoffThrottle();

  // Option parsing
  if (cvmfs::loader_exports_->version >= 3 &&
      cvmfs::loader_exports_->simple_options_parsing) {
    cvmfs::options_manager_ = new SimpleOptionsParser();
  } else {
    cvmfs::options_manager_ = new BashOptionsManager();
  }
  if (loader_exports->config_files != "") {
    vector<string> tokens = SplitString(loader_exports->config_files, ':');
    for (unsigned i = 0, s = tokens.size(); i < s; ++i) {
      cvmfs::options_manager_->ParsePath(tokens[i], false);
    }
  } else {
    cvmfs::options_manager_->ParseDefault(loader_exports->repository_name);
  }
  g_options_ready = true;
  string parameter;

  // Logging
  if (cvmfs::options_manager_->GetValue("CVMFS_SYSLOG_LEVEL", &parameter))
    SetLogSyslogLevel(String2Uint64(parameter));
  else
    SetLogSyslogLevel(3);
  if (cvmfs::options_manager_->GetValue("CVMFS_SYSLOG_FACILITY", &parameter))
    SetLogSyslogFacility(String2Int64(parameter));
  if (cvmfs::options_manager_->GetValue("CVMFS_USYSLOG", &parameter))
    SetLogMicroSyslog(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_DEBUGLOG", &parameter))
    SetLogDebugFile(parameter);
  SetLogSyslogPrefix(loader_exports->repository_name);

  LogCvmfs(kLogCvmfs, kLogDebug, "Options:\n%s",
           cvmfs::options_manager_->Dump().c_str());

  // Overwrite default options
  if (cvmfs::options_manager_->GetValue("CVMFS_MEMCACHE_SIZE", &parameter))
    mem_cache_size = String2Uint64(parameter) * 1024*1024;
  if (cvmfs::options_manager_->GetValue("CVMFS_TIMEOUT", &parameter))
    timeout = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_TIMEOUT_DIRECT", &parameter))
    timeout_direct = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_EXTERNAL_TIMEOUT", &parameter))
    external_timeout = String2Uint64(parameter);
  else
    external_timeout = timeout;
  if (cvmfs::options_manager_->GetValue("CVMFS_EXTERNAL_TIMEOUT_DIRECT",
      &parameter))
    external_timeout_direct = String2Uint64(parameter);
  else
    external_timeout_direct = timeout_direct;
  if (cvmfs::options_manager_->GetValue("CVMFS_LOW_SPEED_LIMIT", &parameter))
    low_speed_limit = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_PROXY_RESET_AFTER", &parameter))
    proxy_reset_after = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_HOST_RESET_AFTER", &parameter))
    host_reset_after = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_MAX_RETRIES", &parameter))
    max_retries = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_BACKOFF_INIT", &parameter))
    backoff_init = String2Uint64(parameter) * 1000;
  if (cvmfs::options_manager_->GetValue("CVMFS_BACKOFF_MAX", &parameter))
    backoff_max = String2Uint64(parameter) * 1000;
  if (cvmfs::options_manager_->GetValue("CVMFS_DNS_TIMEOUT", &parameter))
    dns_timeout_ms = String2Uint64(parameter) * 1000;
  if (cvmfs::options_manager_->GetValue("CVMFS_DNS_RETRIES", &parameter))
    dns_retries = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_SEND_INFO_HEADER", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    send_info_header = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_MAX_IPADDR_PER_PROXY",
                                        &parameter))
  {
    max_ipaddr_per_proxy = String2Uint64(parameter);
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_USE_GEOAPI", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    use_geo_api = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_FOLLOW_REDIRECTS", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    follow_redirects = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_TRACEFILE", &parameter))
    tracefile = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_MAX_TTL", &parameter))
    max_ttl = String2Uint64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_KCACHE_TIMEOUT", &parameter))
    kcache_timeout = String2Int64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_QUOTA_LIMIT", &parameter))
    quota_limit = String2Int64(parameter) * 1024*1024;
  if (cvmfs::options_manager_->GetValue("CVMFS_HTTP_PROXY", &parameter))
    proxies = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_FALLBACK_PROXY", &parameter))
    fallback_proxies = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_EXTERNAL_HTTP_PROXY",
      &parameter))
    external_proxies = parameter;
  else
    external_proxies = "DIRECT";
  if (cvmfs::options_manager_->GetValue("CVMFS_EXTERNAL_FALLBACK_PROXY",
      &parameter))
    fallback_external_proxies = parameter;
  else
    fallback_external_proxies = fallback_proxies;
  if (cvmfs::options_manager_->GetValue("CVMFS_DNS_SERVER", &parameter))
    dns_server = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_EXTERNAL_URL", &parameter)) {
    external_host =
      CalculateHostString(loader_exports->repository_name, parameter);
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_IPFAMILY_PREFER", &parameter))
    ipfamily_prefer = String2Int64(parameter);
  if (cvmfs::options_manager_->GetValue("CVMFS_TRUSTED_CERTS", &parameter))
    trusted_certs = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_PUBLIC_KEY", &parameter)) {
    public_keys = parameter;
  } else if (cvmfs::options_manager_->GetValue("CVMFS_KEYS_DIR", &parameter)) {
    // Collect .pub files from CVMFS_KEYS_DIR
    public_keys = JoinStrings(FindFiles(parameter, ".pub"), ":");
  } else {
    public_keys = JoinStrings(FindFiles("/etc/cvmfs/keys", ".pub"), ":");
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_ROOT_HASH", &parameter))
    root_hash = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_ALT_ROOT_PATH", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    alt_root_path = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_REPOSITORY_TAG", &parameter))
    repository_tag = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_REPOSITORY_DATE", &parameter))
    repository_date = parameter;
  if (cvmfs::options_manager_->GetValue("CVMFS_NFS_SOURCE", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    nfs_source = true;
    if (cvmfs::options_manager_->GetValue("CVMFS_NFS_SHARED", &parameter))
    {
      nfs_shared = true;
      nfs_shared_dir = MakeCanonicalPath(parameter);
    }
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_AUTO_UPDATE", &parameter) &&
      !cvmfs::options_manager_->IsOn(parameter))
  {
    cvmfs::fixed_catalog_ = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_HIDE_MAGIC_XATTRS", &parameter)
      && cvmfs::options_manager_->IsOn(parameter))
  {
    cvmfs::hide_magic_xattrs_ = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_SERVER_CACHE_MODE", &parameter)
      && cvmfs::options_manager_->IsOn(parameter))
  {
    server_cache_mode = true;
    g_raw_symlinks = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_SERVER_URL", &parameter)) {
    hostname = CalculateHostString(loader_exports->repository_name, parameter);
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_CACHE_BASE", &parameter)) {
    cachedir = MakeCanonicalPath(parameter);
    if (cvmfs::options_manager_->GetValue("CVMFS_SHARED_CACHE", &parameter) &&
        cvmfs::options_manager_->IsOn(parameter))
    {
      shared_cache = true;
      cachedir = cachedir + "/shared";
    } else {
      shared_cache = false;
      cachedir = cachedir + "/" + loader_exports->repository_name;
    }
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_ALIEN_CACHE", &parameter)) {
    alien_cache = parameter;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_UID_MAP", &parameter)) {
    retval = uid_map.Read(parameter);
    if (!retval) {
      *g_boot_error = "failed to parse uid map " + parameter;
      return loader::kFailOptions;
    }
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_GID_MAP", &parameter)) {
    retval = gid_map.Read(parameter);
    if (!retval) {
      *g_boot_error = "failed to parse gid map " + parameter;
      return loader::kFailOptions;
    }
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_CLAIM_OWNERSHIP", &parameter) &&
      cvmfs::options_manager_->IsOn(parameter))
  {
    g_claim_ownership = true;
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_INITIAL_GENERATION",
                                        &parameter)) {
    initial_generation = String2Uint64(parameter);
  }
  if (cvmfs::options_manager_->GetValue("CVMFS_PROXY_TEMPLATE", &parameter)) {
    proxy_template = parameter;
  }

  cvmfs::statistics_ = new perf::Statistics();

  // register the ShortString's static counters
  cvmfs::statistics_->Register("pathstring.n_instances", "Number of instances");
  cvmfs::statistics_->Register("pathstring.n_overflows", "Number of overflows");
  cvmfs::statistics_->Register("namestring.n_instances", "Number of instances");
  cvmfs::statistics_->Register("namestring.n_overflows", "Number of overflows");
  cvmfs::statistics_->Register("linkstring.n_instances", "Number of instances");
  cvmfs::statistics_->Register("linkstring.n_overflows", "Number of overflows");

  // register inode tracker counters
  cvmfs::statistics_->Register(
    "inode_tracker.n_insert", "overall number of accessed inodes");
  cvmfs::statistics_->Register(
    "inode_tracker.n_remove", "overall number of evicted inodes");
  cvmfs::statistics_->Register(
    "inode_tracker.no_reference", "currently active inodes");
  cvmfs::statistics_->Register(
    "inode_tracker.n_hit_inode", "overall number of inode lookups");
  cvmfs::statistics_->Register(
    "inode_tracker.n_hit_path", "overall number of successful path lookups");
  cvmfs::statistics_->Register(
    "inode_tracker.n_miss_path", "overall number of unsuccessful path lookups");

  // Fill cvmfs option variables from configuration
  cvmfs::foreground_ = loader_exports->foreground;
  cvmfs::cachedir_ = new string(cachedir);
  cvmfs::nfs_shared_dir_ = new string(nfs_shared_dir);
  cvmfs::tracefile_ = new string(tracefile);
  cvmfs::repository_name_ = new string(loader_exports->repository_name);
  cvmfs::repository_tag_ = new string(repository_tag);
  cvmfs::mountpoint_ = new string(loader_exports->mount_point);
  g_uid = geteuid();
  g_gid = getegid();
  cvmfs::SetMaxTTL(max_ttl);
  if (kcache_timeout) {
    cvmfs::kcache_timeout_ =
      (kcache_timeout == -1) ? 0.0 : static_cast<double>(kcache_timeout);
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "kernel caches expire after %d seconds",
           static_cast<int>(cvmfs::kcache_timeout_));

  // Tune SQlite3
  sqlite3_shutdown();  // Make sure SQlite starts clean after initialization
  retval = sqlite3_config(SQLITE_CONFIG_LOG, LogSqliteError, NULL);
  assert(retval == SQLITE_OK);
  retval = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  assert(retval == SQLITE_OK);
  // 8 KB for 8 threads (2 slots per thread)
  g_sqlite_scratch = smalloc(8192*16);
  g_sqlite_page_cache = smalloc(1280*3275);  // 4MB
  retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, g_sqlite_scratch, 8192, 16);
  assert(retval == SQLITE_OK);
  retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, g_sqlite_page_cache,
                          1280, 3275);
  assert(retval == SQLITE_OK);
  // 4 KB
  retval = sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 32, 128);
  assert(retval == SQLITE_OK);

  // Disable SQlite3 locks
  retval = sqlite3_vfs_register(sqlite3_vfs_find("unix-none"), 1);
  assert(retval == SQLITE_OK);

  // Meta-data memory caches
  const double memcache_unit_size =
    7.0 * lru::Md5PathCache::GetEntrySize() +
    lru::InodeCache::GetEntrySize() + lru::PathCache::GetEntrySize();
  const unsigned memcache_num_units =
    mem_cache_size / static_cast<unsigned>(memcache_unit_size);
  // Number of cache entries must be a multiple of 64
  const unsigned mask_64 = ~((1 << 6) - 1);
  cvmfs::inode_cache_ = new lru::InodeCache(memcache_num_units & mask_64,
      cvmfs::statistics_);
  cvmfs::path_cache_ = new lru::PathCache(memcache_num_units & mask_64,
      cvmfs::statistics_);
  cvmfs::md5path_cache_ =
    new lru::Md5PathCache((memcache_num_units*7) & mask_64,
        cvmfs::statistics_);
  cvmfs::inode_tracker_ = new glue::InodeTracker();

  cvmfs::directory_handles_ = new cvmfs::DirectoryHandles();
  cvmfs::directory_handles_->set_empty_key((uint64_t)(-1));
  cvmfs::directory_handles_->set_deleted_key((uint64_t)(-2));
  cvmfs::chunk_tables_ = new ChunkTables();

  // Runtime counters
  cvmfs::n_fs_open_ = cvmfs::statistics_->Register("cvmfs.n_fs_open",
      "Overall number of file open operations");
  cvmfs::n_fs_dir_open_ = cvmfs::statistics_->Register("cvmfs.n_fs_dir_open",
      "Overall number of directory open operations");
  cvmfs::n_fs_lookup_ = cvmfs::statistics_->Register("cvmfs.n_fs_lookup",
      "Number of lookups");
  cvmfs::n_fs_lookup_negative_ = cvmfs::statistics_->Register(
      "cvmfs.n_fs_lookup_negative", "Number of negative lookups");
  cvmfs::n_fs_stat_ = cvmfs::statistics_->Register("cvmfs.n_fs_stat",
      "Number of stats");
  cvmfs::n_fs_read_ = cvmfs::statistics_->Register("cvmfs.n_fs_read",
      "Number of files read");
  cvmfs::n_fs_readlink_ = cvmfs::statistics_->Register("cvmfs.n_fs_readlink",
      "Number of links read");
  cvmfs::n_fs_forget_ = cvmfs::statistics_->Register("cvmfs.n_fs_forget",
      "Number of inode forgets");
  cvmfs::n_io_error_ = cvmfs::statistics_->Register("cvmfs.n_io_error",
      "Number of I/O errors");

  // Create cache directory, if necessary
  if (!MkdirDeep(*cvmfs::cachedir_, 0700, false)) {
    *g_boot_error = "cannot create cache directory " + *cvmfs::cachedir_;
    return loader::kFailCacheDir;
  }

  // Try to jump to cache directory.  This tests, if it is accassible.
  // Also, it brings speed later on.
  if (chdir(cvmfs::cachedir_->c_str()) != 0) {
    *g_boot_error = "cache directory " + *cvmfs::cachedir_ + " is unavailable";
    return loader::kFailCacheDir;
  }

  // Create lock file and running sentinel
  g_fd_lockfile = TryLockFile("lock." + *cvmfs::repository_name_);
  if (g_fd_lockfile == -1) {
    *g_boot_error = "could not acquire lock (" + StringifyInt(errno) + ")";
    return loader::kFailCacheDir;
  } else if (g_fd_lockfile == -2) {
    // Prevent double mount
    string fqrn;
    retval = platform_getxattr(*cvmfs::mountpoint_, "user.fqrn", &fqrn);
    if (!retval) {
      g_fd_lockfile = LockFile("lock." + *cvmfs::repository_name_);
      if (g_fd_lockfile < 0) {
        *g_boot_error = "could not acquire lock (" + StringifyInt(errno) + ")";
        return loader::kFailCacheDir;
      }
    } else {
      if (fqrn == *cvmfs::repository_name_) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
                 "repository already mounted on %s",
                 cvmfs::mountpoint_->c_str());
        return loader::kFailDoubleMount;
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
                 "CernVM-FS repository %s already mounted on %s",
                 fqrn.c_str(), cvmfs::mountpoint_->c_str());
        return loader::kFailOtherMount;
      }
    }
  }
  platform_stat64 info;
  if (platform_stat(("running." + *cvmfs::repository_name_).c_str(),
                    &info) == 0)
  {
    string msg;
    if (quota_limit > 0) {
      msg = "looks like cvmfs has been crashed previously, "
            "rebuilding cache database";
    } else {
      msg = "looks like cvmfs has been crashed previously";
    }
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn, "%s", msg.c_str());
    rebuild_cachedb = true;
  }
  retval = open(("running." + *cvmfs::repository_name_).c_str(),
                O_RDONLY | O_CREAT, 0600);
  if (retval < 0) {
    *g_boot_error = "could not open running sentinel (" +
                    StringifyInt(errno) + ")";
    return loader::kFailCacheDir;
  }
  close(retval);
  g_running_created = true;

  // Redirect SQlite temp directory to cache (global variable)
  sqlite3_temp_directory =
    static_cast<char *>(sqlite3_malloc(strlen(".") + 1));
  snprintf(sqlite3_temp_directory, strlen(".") + 1, ".");

  // Creates a set of cache directories (256 directories named 00..ff)
  if (alien_cache != ".") {
    if (shared_cache || quota_limit > 0) {
      *g_boot_error = "Failure: quota management and alien cache mutually "
                      "exclusive.  Turn of quota limit and shared cache.";
      return loader::kFailCacheDir;
    }
  }
  cvmfs::cache_manager_ = cache::PosixCacheManager::Create(
    alien_cache, alien_cache != ".", server_cache_mode);
  if (cvmfs::cache_manager_ == NULL) {
    *g_boot_error = "Failed to setup cache in " + alien_cache +
                    ": " + strerror(errno);
    return loader::kFailCacheDir;
  }
  CreateFile("./.cvmfscache", 0600);

  // Init quota / managed cache
  if (quota_limit > 0) {
    int64_t quota_threshold = quota_limit/2;
    PosixQuotaManager *quota_mgr;
    if (shared_cache) {
      quota_mgr = PosixQuotaManager::CreateShared(
        loader_exports->program_name, ".", quota_limit, quota_threshold);
      if (quota_mgr == NULL) {
        *g_boot_error = "Failed to initialize shared lru cache";
        return loader::kFailQuota;
      }
    } else {
      quota_mgr = PosixQuotaManager::Create(
        ".", quota_limit, quota_threshold, rebuild_cachedb);
      if (quota_mgr == NULL) {
        *g_boot_error = "Failed to initialize lru cache";
        return loader::kFailQuota;
      }
    }

    if (quota_mgr->GetSize() > quota_mgr->GetCapacity()) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
               "cache is already beyond quota size "
               "(size: %"PRId64", capacity: %"PRId64"), cleaning up",
               quota_mgr->GetSize(), quota_mgr->GetCapacity());
      if (!quota_mgr->Cleanup(quota_threshold)) {
        delete quota_mgr;
        *g_boot_error = "Failed to clean up cache";
        return loader::kFailQuota;
      }
    }

    retval = cvmfs::cache_manager_->AcquireQuotaManager(quota_mgr);
    assert(retval);
    LogCvmfs(kLogCvmfs, kLogDebug,
             "CernVM-FS: quota initialized, current size %luMB",
             quota_mgr->GetSize()/(1024*1024));
  }
  g_quota_ready = true;

  // Start NFS maps module, if necessary
#ifdef CVMFS_NFS_SUPPORT
  if (nfs_source) {
    if (FileExists("./no_nfs_maps." + (*cvmfs::repository_name_))) {
      *g_boot_error = "Cache was used without NFS maps before. "
                      "It has to be wiped out.";
      return loader::kFailNfsMaps;
    }

    cvmfs::nfs_maps_ = true;

    string inode_cache_dir = "./nfs_maps." + (*cvmfs::repository_name_);
    if (nfs_shared) {
      inode_cache_dir = (*cvmfs::nfs_shared_dir_) + "/nfs_maps."
                          + (*cvmfs::repository_name_);
    }
    if (!MkdirDeep(inode_cache_dir, 0700)) {
      *g_boot_error = "Failed to initialize NFS maps";
      return loader::kFailNfsMaps;
    }
    if (!nfs_maps::Init(inode_cache_dir,
                        catalog::ClientCatalogManager::kInodeOffset+1,
                        rebuild_cachedb, nfs_shared))
    {
      *g_boot_error = "Failed to initialize NFS maps";
      return loader::kFailNfsMaps;
    }
    g_nfs_maps_ready = true;
  } else {
    CreateFile("./no_nfs_maps." + (*cvmfs::repository_name_), 0600);
  }
#endif

  // Monitor, check for maximum number of open files
  if (cvmfs::UseWatchdog()) {
    if (!monitor::Init(".", *cvmfs::repository_name_, true)) {
      *g_boot_error = "failed to initialize watchdog.";
      return loader::kFailMonitor;
    }
    g_monitor_ready = true;
  }
  cvmfs::max_open_files_ = monitor::GetMaxOpenFiles();
  cvmfs::no_open_files_ = cvmfs::statistics_->Register("cvmfs.no_open_files",
      "Number of currently opened files");
  cvmfs::no_open_dirs_ = cvmfs::statistics_->Register("cvmfs.no_open_dirs",
      "Number of currently opened directories");

  // Control & command interface
  if (!talk::Init(".", cvmfs::options_manager_)) {
    *g_boot_error = "failed to initialize talk socket (" +
                    StringifyInt(errno) + ")";
    return loader::kFailTalk;
  }
  g_talk_ready = true;

  // Uuid: create or load from cache (only for proxy template)
  uuid = cvmfs::Uuid::Create("./uuid");
  if (uuid == NULL) {
    *g_boot_error = "failed to load/store uuid";
    return loader::kFailCacheDir;
  }

  // Network initialization
  cvmfs::download_manager_ = new download::DownloadManager();
  cvmfs::download_manager_->Init(cvmfs::kDefaultNumConnections, false,
      cvmfs::statistics_);
  cvmfs::download_manager_->SetHostChain(hostname);
  if ((dns_timeout_ms != download::DownloadManager::kDnsDefaultTimeoutMs) ||
      (dns_retries != download::DownloadManager::kDnsDefaultRetries))
  {
    cvmfs::download_manager_->SetDnsParameters(dns_retries, dns_timeout_ms);
  }
  if (!dns_server.empty()) {
    cvmfs::download_manager_->SetDnsServer(dns_server);
  }
  if (follow_redirects) {
    cvmfs::download_manager_->EnableRedirects();
  }
  cvmfs::download_manager_->SetTimeout(timeout, timeout_direct);
  cvmfs::download_manager_->SetLowSpeedLimit(low_speed_limit);
  cvmfs::download_manager_->SetProxyGroupResetDelay(proxy_reset_after);
  cvmfs::download_manager_->SetHostResetDelay(host_reset_after);
  cvmfs::download_manager_->SetRetryParameters(max_retries,
                                               backoff_init,
                                               backoff_max);
  cvmfs::download_manager_->SetMaxIpaddrPerProxy(max_ipaddr_per_proxy);
  cvmfs::download_manager_->SetProxyTemplates(uuid->uuid(), proxy_template);
  if (ipfamily_prefer != 0) {
    switch (ipfamily_prefer) {
      case 4:
        cvmfs::download_manager_->SetIpPreference(dns::kIpPreferV4);
        break;
      case 6:
        cvmfs::download_manager_->SetIpPreference(dns::kIpPreferV6);
        break;
    }
  }
  if (send_info_header)
    cvmfs::download_manager_->EnableInfoHeader();
  proxies = download::ResolveProxyDescription(proxies,
                                              cvmfs::download_manager_);
  if (proxies == "") {
    *g_boot_error = "failed to discover HTTP proxy servers";
    return loader::kFailWpad;
  }
  cvmfs::download_manager_->SetProxyChain(
    proxies, fallback_proxies, download::DownloadManager::kSetProxyBoth);
  g_download_ready = true;
  if (use_geo_api) {
    cvmfs::download_manager_->ProbeGeo();
  }

  // Initialize the _external_ download manager.  Mostly the same as the
  // primary except it has a different hostname and timeout.
  cvmfs::external_download_manager_ = new download::DownloadManager();
  cvmfs::external_download_manager_->Init(cvmfs::kDefaultNumConnections, false,
      cvmfs::statistics_, "download-external");

  cvmfs::external_download_manager_->SetHostChain(!external_host.empty() ?
                                                  external_host : hostname);
  if ((dns_timeout_ms != download::DownloadManager::kDnsDefaultTimeoutMs) ||
      (dns_retries != download::DownloadManager::kDnsDefaultRetries))
  {
    cvmfs::external_download_manager_->SetDnsParameters(dns_retries,
                                                        dns_timeout_ms);
  }
  if (!dns_server.empty()) {
    cvmfs::external_download_manager_->SetDnsServer(dns_server);
  }
  if (follow_redirects) {
    cvmfs::external_download_manager_->EnableRedirects();
  }
  cvmfs::external_download_manager_->SetTimeout(external_timeout,
                                                external_timeout_direct);
  cvmfs::external_download_manager_->SetLowSpeedLimit(low_speed_limit);
  cvmfs::external_download_manager_->SetProxyGroupResetDelay(proxy_reset_after);
  cvmfs::external_download_manager_->SetHostResetDelay(host_reset_after);
  cvmfs::external_download_manager_->SetRetryParameters(max_retries,
                                               backoff_init,
                                               backoff_max);
  cvmfs::external_download_manager_->SetMaxIpaddrPerProxy(max_ipaddr_per_proxy);
  cvmfs::external_download_manager_->SetProxyTemplates(uuid->uuid(),
                                                       proxy_template);
  delete uuid;
  uuid = NULL;
  if (ipfamily_prefer != 0) {
    switch (ipfamily_prefer) {
      case 4:
        cvmfs::external_download_manager_->SetIpPreference(dns::kIpPreferV4);
        break;
      case 6:
        cvmfs::external_download_manager_->SetIpPreference(dns::kIpPreferV6);
        break;
    }
  }
  if (send_info_header)
    cvmfs::external_download_manager_->EnableInfoHeader();
  external_proxies = download::ResolveProxyDescription(external_proxies,
                                            cvmfs::external_download_manager_);
  if (external_proxies == "") {
    *g_boot_error = "failed to discover HTTP proxy servers";
    return loader::kFailWpad;
  }
  cvmfs::external_download_manager_->SetProxyChain(
    external_proxies, fallback_external_proxies,
    download::DownloadManager::kSetProxyBoth);
  g_external_download_ready = true;
  if (use_geo_api) {
    std::vector<std::string> host_chain;
    // If no external host was specified, reuse the geo API ordering
    // of the regular download manager.
    if (external_host.empty()) {
      cvmfs::download_manager_->GetHostInfo(&host_chain, NULL, NULL);
      cvmfs::external_download_manager_->SetHostChain(host_chain);
    } else {
      cvmfs::external_download_manager_->GetHostInfo(&host_chain, NULL, NULL);
      cvmfs::download_manager_->GeoSortServers(&host_chain);
      cvmfs::external_download_manager_->SetHostChain(host_chain);
    }
  }


  cvmfs::signature_manager_ = new signature::SignatureManager();
  cvmfs::signature_manager_->Init();
  if (!cvmfs::signature_manager_->LoadPublicRsaKeys(public_keys)) {
    *g_boot_error = "failed to load public key(s)";
    return loader::kFailSignature;
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "CernVM-FS: using public key(s) %s",
             public_keys.c_str());
  }
  if (trusted_certs != "") {
    if (!cvmfs::signature_manager_->LoadTrustedCaCrl(trusted_certs)) {
      *g_boot_error = "failed to load trusted certificates";
      return loader::kFailSignature;
    }
  }
  g_signature_ready = true;
  string config_repository_path = "";
  if (FileExists("/etc/cvmfs/blacklist")) {
    const bool append = false;
    if (!cvmfs::signature_manager_->LoadBlacklist("/etc/cvmfs/blacklist",
                                                  append))
    {
      *g_boot_error = "failed to load blacklist";
      return loader::kFailSignature;
    }
  }
  if (cvmfs::options_manager_->HasConfigRepository(*cvmfs::repository_name_,
                                                   &config_repository_path)
      && FileExists(config_repository_path + "blacklist"))
  {
    const bool append = true;
    if (!cvmfs::signature_manager_->LoadBlacklist(
          config_repository_path + "blacklist", append))
    {
      *g_boot_error = "failed to load blacklist from config repository";
      return loader::kFailSignature;
    }
  }

  cvmfs::fetcher_ = new cvmfs::Fetcher(
    cvmfs::cache_manager_,
    cvmfs::download_manager_,
    cvmfs::backoff_throttle_,
    cvmfs::statistics_);

  const bool is_external_data = true;
  cvmfs::external_fetcher_ = new cvmfs::Fetcher(
    cvmfs::cache_manager_,
    cvmfs::external_download_manager_,
    cvmfs::backoff_throttle_,
    cvmfs::statistics_,
    "fetch-external",
    is_external_data);

  // Load initial file catalog
  LogCvmfs(kLogCvmfs, kLogDebug, "fuse inode size is %d bits",
           sizeof(fuse_ino_t) * 8);
  cvmfs::inode_annotation_ = new catalog::InodeGenerationAnnotation();
  cvmfs::inode_annotation_->IncGeneration(initial_generation);
  cvmfs::catalog_manager_ = new catalog::ClientCatalogManager(
    *cvmfs::repository_name_,
    cvmfs::fetcher_,
    cvmfs::signature_manager_,
    cvmfs::statistics_);
  if (!nfs_source) {
    cvmfs::catalog_manager_->SetInodeAnnotation(cvmfs::inode_annotation_);
  }
  cvmfs::catalog_manager_->SetOwnerMaps(uid_map, gid_map);

  // Load specific tag (root hash has precedence, then repository_tag)
  if ((root_hash == "") &&
      ((*cvmfs::repository_tag_ != "") || (repository_date != "")))
  {
    manifest::Failures retval_mf;
    download::Failures retval_dl;
    manifest::ManifestEnsemble ensemble;
    retval_mf = manifest::Fetch("", *cvmfs::repository_name_, 0, NULL,
                                cvmfs::signature_manager_,
                                cvmfs::download_manager_,
                                &ensemble);
    if (retval_mf != manifest::kFailOk) {
      *g_boot_error = "Failed to fetch manifest";
      return loader::kFailHistory;
    }
    shash::Any history_hash = ensemble.manifest->history();
    if (history_hash.IsNull()) {
      *g_boot_error = "No history";
      return loader::kFailHistory;
    }
    string history_path = "txn/historydb" + history_hash.ToString() + "." +
                          *cvmfs::repository_name_;
    string history_url = "/data/" + history_hash.MakePath();
    download::JobInfo download_history(&history_url, true, true, &history_path,
                                       &history_hash);
    retval_dl = cvmfs::download_manager_->Fetch(&download_history);
    if (retval_dl != download::kFailOk) {
      *g_boot_error = "failed to download history: " + StringifyInt(retval_dl);
      return loader::kFailHistory;
    }
    UnlinkGuard history_file(history_path);
    UniquePtr<history::History> tag_db(
      history::SqliteHistory::Open(history_path));
    if (!tag_db) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
               "failed to open history database (%s)", history_path.c_str());
      return loader::kFailHistory;
    }
    history::History::Tag tag;
    if (*cvmfs::repository_tag_ == "") {
      time_t repository_utctime = IsoTimestamp2UtcTime(repository_date);
      if (repository_utctime == 0) {
        *g_boot_error = "invalid timestamp in CVMFS_REPOSITORY_DATE: " +
                        repository_date + ". Use YYYY-MM-DDTHH:MM:SSZ";
        return loader::kFailHistory;
      }
      retval = tag_db->GetByDate(repository_utctime, &tag);
      if (!retval) {
        *g_boot_error = "no repository state as early as utc timestamp " +
                        StringifyTime(repository_utctime, true);
        return loader::kFailHistory;
      }
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
               "time stamp %s UTC resolved to tag '%s'",
               StringifyTime(repository_utctime, true).c_str(),
               tag.name.c_str());
      *cvmfs::repository_tag_ = tag.name;
    } else {
      retval = tag_db->GetByName(*cvmfs::repository_tag_, &tag);
      if (!retval) {
        *g_boot_error = "no such tag: " + *cvmfs::repository_tag_;
        return loader::kFailHistory;
      }
    }
    root_hash = tag.root_hash.ToString();
  }

  retval = sqlite::RegisterVfsRdOnly(
    cvmfs::cache_manager_, cvmfs::statistics_, sqlite::kVfsOptDefault);
  assert(retval);

  if (root_hash != "") {
    cvmfs::fixed_catalog_ = true;
    shash::Any hash = MkFromHexPtr(shash::HexPtr(string(root_hash)),
                                   shash::kSuffixCatalog);
    retval = cvmfs::catalog_manager_->InitFixed(hash, alt_root_path);
  } else {
    retval = cvmfs::catalog_manager_->Init();
  }
  if (!retval) {
    *g_boot_error = "Failed to initialize root file catalog";
    return loader::kFailCatalog;
  }
  cvmfs::inode_generation_info_.initial_revision =
    cvmfs::catalog_manager_->GetRevision();
  cvmfs::inode_generation_info_.inode_generation =
    cvmfs::inode_annotation_->GetGeneration();
  LogCvmfs(kLogCvmfs, kLogDebug, "root inode is %"PRIu64,
           uint64_t(cvmfs::catalog_manager_->GetRootInode()));

  if (cvmfs::catalog_manager_->GetVolatileFlag()) {
    LogCvmfs(kLogCvmfs, kLogDebug, "content of repository flagged as VOLATILE");
    cvmfs::volatile_repository_ = true;
  }

  cvmfs::voms_authz_ = new string();
  cvmfs::has_voms_authz_ =
    cvmfs::catalog_manager_->GetVOMSAuthz(cvmfs::voms_authz_);

  // Make sure client context TLS has been initialized
  // (first initialization is not thread safe).
  ClientCtx::GetInstance();

  cvmfs::pipe_remount_trigger_[0] = cvmfs::pipe_remount_trigger_[1] = -1;
  cvmfs::remount_fence_ = new cvmfs::RemountFence();
  auto_umount::SetMountpoint(*cvmfs::mountpoint_);

  return loader::kFailOk;
}  // NOLINT TODO(jblomer): disentangle


/**
 * Things that have to be executed after fork() / daemon()
 */
static void Spawn() {
  int retval;

  // First thing: fork off the watchdog while we still have a single-threaded
  // well-defined state
  cvmfs::pid_ = getpid();
  if (cvmfs::UseWatchdog() && g_monitor_ready) {
    monitor::RegisterOnCrash(auto_umount::UmountOnCrash);
    monitor::Spawn();
  }

  // Setup catalog reload alarm (_after_ forking into daemon mode)
  atomic_init32(&cvmfs::maintenance_mode_);
  atomic_init32(&cvmfs::drainout_mode_);
  atomic_init32(&cvmfs::reload_critical_section_);
  atomic_init32(&cvmfs::catalogs_expired_);
  if (!cvmfs::fixed_catalog_) {
    MakePipe(cvmfs::pipe_remount_trigger_);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = cvmfs::AlarmReload;
    sa.sa_flags = SA_RESTART | SA_SIGINFO;
    sigfillset(&sa.sa_mask);
    retval = sigaction(SIGALRM, &sa, NULL);
    assert(retval == 0);
    unsigned ttl = cvmfs::catalog_manager_->offline_mode() ?
      cvmfs::kShortTermTTL : cvmfs::GetEffectiveTTL();
    alarm(ttl);
    cvmfs::catalogs_valid_until_ = time(NULL) + ttl;

    cvmfs::thread_remount_trigger_ = reinterpret_cast<pthread_t *>(
      smalloc(sizeof(pthread_t)));
    retval = pthread_create(cvmfs::thread_remount_trigger_, NULL,
                            cvmfs::MainRemountTrigger, NULL);
    assert(retval == 0);
  } else {
    cvmfs::catalogs_valid_until_ = cvmfs::kIndefiniteDeadline;
  }

  cvmfs::download_manager_->Spawn();
  cvmfs::external_download_manager_->Spawn();
  cvmfs::cache_manager_->quota_mgr()->Spawn();
  if (cvmfs::cache_manager_->quota_mgr()->IsEnforcing()) {
    cvmfs::watchdog_listener_ = quota::RegisterWatchdogListener(
      cvmfs::cache_manager_->quota_mgr(),
      *cvmfs::repository_name_ + "-watchdog");
    cvmfs::unpin_listener_ = quota::RegisterUnpinListener(
      cvmfs::cache_manager_->quota_mgr(),
      cvmfs::catalog_manager_,
      *cvmfs::repository_name_ + "-unpin");
  }
  talk::Spawn();
  if (cvmfs::nfs_maps_)
    nfs_maps::Spawn();

  if (*cvmfs::tracefile_ != "")
    tracer::Init(8192, 7000, *cvmfs::tracefile_);
  else
    tracer::InitNull();
}


static string GetErrorMsg() {
  if (g_boot_error)
    return *g_boot_error;
  return "";
}


static void Fini() {
  signal(SIGALRM, SIG_IGN);
  if (cvmfs::thread_remount_trigger_) {
    char quit = 'Q';
    WritePipe(cvmfs::pipe_remount_trigger_[1], &quit, 1);
    pthread_join(*cvmfs::thread_remount_trigger_, NULL);
    free(cvmfs::thread_remount_trigger_);
    cvmfs::thread_remount_trigger_ = NULL;
    ClosePipe(cvmfs::pipe_remount_trigger_);
    cvmfs::pipe_remount_trigger_[0] = cvmfs::pipe_remount_trigger_[1] = -1;
  }

  if (g_talk_ready) talk::Fini();

  // The unpin listener requires the catalog, so this must be unregistered
  // before the catalog manager is removed
  if (g_quota_ready) {
    if (cvmfs::unpin_listener_) {
      quota::UnregisterListener(cvmfs::unpin_listener_);
      cvmfs::unpin_listener_ = NULL;
    }
    if (cvmfs::watchdog_listener_) {
      quota::UnregisterListener(cvmfs::watchdog_listener_);
      cvmfs::watchdog_listener_ = NULL;
    }
  }

  // Must be before cache and quota are stopped
  delete cvmfs::catalog_manager_;
  cvmfs::catalog_manager_ = NULL;

  if (cvmfs::fetcher_) {
    delete cvmfs::fetcher_;
    cvmfs::fetcher_ = NULL;
  }

  tracer::Fini();
  if (g_signature_ready) cvmfs::signature_manager_->Fini();
  if (g_download_ready) cvmfs::download_manager_->Fini();
  if (g_external_download_ready) cvmfs::external_download_manager_->Fini();
  if (g_nfs_maps_ready) nfs_maps::Fini();
  if (cvmfs::cache_manager_) {
    delete cvmfs::cache_manager_;
    cvmfs::cache_manager_ = NULL;
  }
  if (g_running_created)
    unlink(("running." + *cvmfs::repository_name_).c_str());
  if (g_fd_lockfile >= 0) UnlockFile(g_fd_lockfile);
  if (g_options_ready) {
    delete cvmfs::options_manager_;
    cvmfs::options_manager_ = NULL;
  }

  delete cvmfs::remount_fence_;
  delete cvmfs::signature_manager_;
  delete cvmfs::download_manager_;
  delete cvmfs::external_download_manager_;
  delete cvmfs::inode_annotation_;
  delete cvmfs::directory_handles_;
  delete cvmfs::chunk_tables_;
  delete cvmfs::inode_tracker_;
  delete cvmfs::path_cache_;
  delete cvmfs::inode_cache_;
  delete cvmfs::md5path_cache_;
  delete cvmfs::cachedir_;
  delete cvmfs::nfs_shared_dir_;
  delete cvmfs::tracefile_;
  delete cvmfs::repository_name_;
  delete cvmfs::repository_tag_;
  delete cvmfs::mountpoint_;
  delete cvmfs::voms_authz_;
  cvmfs::remount_fence_ = NULL;
  cvmfs::signature_manager_ = NULL;
  cvmfs::download_manager_ = NULL;
  cvmfs::external_download_manager_ = NULL;
  cvmfs::inode_annotation_ = NULL;
  cvmfs::directory_handles_ = NULL;
  cvmfs::chunk_tables_ = NULL;
  cvmfs::inode_tracker_ = NULL;
  cvmfs::path_cache_ = NULL;
  cvmfs::inode_cache_ = NULL;
  cvmfs::md5path_cache_ = NULL;
  cvmfs::cachedir_ = NULL;
  cvmfs::nfs_shared_dir_ = NULL;
  cvmfs::tracefile_ = NULL;
  cvmfs::repository_name_ = NULL;
  cvmfs::repository_tag_ = NULL;
  cvmfs::mountpoint_ = NULL;
  cvmfs::voms_authz_ = NULL;

  sqlite::UnregisterVfsRdOnly();
  if (sqlite3_temp_directory) {
    sqlite3_free(sqlite3_temp_directory);
    sqlite3_temp_directory = NULL;
  }
  sqlite3_shutdown();
  if (g_sqlite_page_cache) free(g_sqlite_page_cache);
  if (g_sqlite_scratch) free(g_sqlite_scratch);
  g_sqlite_page_cache = NULL;
  g_sqlite_scratch = NULL;

  if (g_monitor_ready) monitor::Fini();

  delete g_boot_error;
  g_boot_error = NULL;
  SetLogSyslogPrefix("");
  SetLogMicroSyslog("");
  SetLogDebugFile("");
  auto_umount::SetMountpoint("");

  delete cvmfs::backoff_throttle_;
  cvmfs::backoff_throttle_ = NULL;
  delete cvmfs::statistics_;
  cvmfs::statistics_ = NULL;

  // Make sure client context TLS is cleaned up
  // (destruction is not thread safe)
  ClientCtx::CleanupInstance();
}


static int AltProcessFlavor(int argc, char **argv) {
  if (strcmp(argv[1], "__cachemgr__") == 0) {
    return PosixQuotaManager::MainCacheManager(argc, argv);
  }
  if (strcmp(argv[1], "__wpad__") == 0) {
    return download::MainResolveProxyDescription(argc, argv);
  }
#ifdef VOMS_AUTHZ
  if (strcmp(argv[1], "__cred_fetcher__") == 0) {
    return CredentialsFetcher::MainCredentialsFetcher(argc, argv);
  }
#endif
  return 1;
}


static bool MaintenanceMode(const int fd_progress) {
  SendMsg2Socket(fd_progress, "Entering maintenance mode\n");
  signal(SIGALRM, SIG_IGN);
  atomic_cas32(&cvmfs::maintenance_mode_, 0, 1);
  string msg_progress =
    "Draining out kernel caches (" +
    StringifyInt(static_cast<int>(cvmfs::kcache_timeout_)) + "s)\n";
  SendMsg2Socket(fd_progress, msg_progress);
  SafeSleepMs(static_cast<int>(
              cvmfs::kcache_timeout_*1000 + cvmfs::kReloadSafetyMargin));
  return true;
}


static bool SaveState(const int fd_progress, loader::StateList *saved_states) {
  string msg_progress;

  unsigned num_open_dirs = cvmfs::directory_handles_->size();
  if (num_open_dirs != 0) {
#ifdef DEBUGMSG
    for (cvmfs::DirectoryHandles::iterator i =
         cvmfs::directory_handles_->begin(),
         iEnd = cvmfs::directory_handles_->end(); i != iEnd; ++i)
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "saving dirhandle %d", i->first);
    }
#endif

    msg_progress = "Saving open directory handles (" +
      StringifyInt(num_open_dirs) + " handles)\n";
    SendMsg2Socket(fd_progress, msg_progress);

    // TODO(jblomer): should rather be saved just in a malloc'd memory block
    cvmfs::DirectoryHandles *saved_handles =
      new cvmfs::DirectoryHandles(*cvmfs::directory_handles_);
    loader::SavedState *save_open_dirs = new loader::SavedState();
    save_open_dirs->state_id = loader::kStateOpenDirs;
    save_open_dirs->state = saved_handles;
    saved_states->push_back(save_open_dirs);
  }

  if (!cvmfs::nfs_maps_) {
    msg_progress = "Saving inode tracker\n";
    SendMsg2Socket(fd_progress, msg_progress);
    glue::InodeTracker *saved_inode_tracker =
      new glue::InodeTracker(*cvmfs::inode_tracker_);
    loader::SavedState *state_glue_buffer = new loader::SavedState();
    state_glue_buffer->state_id = loader::kStateGlueBufferV4;
    state_glue_buffer->state = saved_inode_tracker;
    saved_states->push_back(state_glue_buffer);
  }

  msg_progress = "Saving chunk tables\n";
  SendMsg2Socket(fd_progress, msg_progress);
  ChunkTables *saved_chunk_tables = new ChunkTables(*cvmfs::chunk_tables_);
  loader::SavedState *state_chunk_tables = new loader::SavedState();
  state_chunk_tables->state_id = loader::kStateOpenFilesV3;
  state_chunk_tables->state = saved_chunk_tables;
  saved_states->push_back(state_chunk_tables);

  msg_progress = "Saving inode generation\n";
  SendMsg2Socket(fd_progress, msg_progress);
  cvmfs::inode_generation_info_.inode_generation +=
    cvmfs::catalog_manager_->inode_gauge();
  cvmfs::InodeGenerationInfo *saved_inode_generation =
    new cvmfs::InodeGenerationInfo(cvmfs::inode_generation_info_);
  loader::SavedState *state_inode_generation = new loader::SavedState();
  state_inode_generation->state_id = loader::kStateInodeGeneration;
  state_inode_generation->state = saved_inode_generation;
  saved_states->push_back(state_inode_generation);

  msg_progress = "Saving open files counter\n";
  SendMsg2Socket(fd_progress, msg_progress);
  uint32_t *saved_num_fd = new uint32_t(cvmfs::no_open_files_->Get());
  loader::SavedState *state_num_fd = new loader::SavedState();
  state_num_fd->state_id = loader::kStateOpenFilesCounter;
  state_num_fd->state = saved_num_fd;
  saved_states->push_back(state_num_fd);

  return true;
}


static bool RestoreState(const int fd_progress,
                         const loader::StateList &saved_states)
{
  for (unsigned i = 0, l = saved_states.size(); i < l; ++i) {
    if (saved_states[i]->state_id == loader::kStateOpenDirs) {
      SendMsg2Socket(fd_progress, "Restoring open directory handles... ");
      delete cvmfs::directory_handles_;
      cvmfs::DirectoryHandles *saved_handles =
        (cvmfs::DirectoryHandles *)saved_states[i]->state;
      cvmfs::directory_handles_ = new cvmfs::DirectoryHandles(*saved_handles);
      cvmfs::no_open_dirs_->Set(cvmfs::directory_handles_->size());
      cvmfs::DirectoryHandles::const_iterator i =
        cvmfs::directory_handles_->begin();
      for (; i != cvmfs::directory_handles_->end(); ++i) {
        if (i->first >= cvmfs::next_directory_handle_)
          cvmfs::next_directory_handle_ = i->first + 1;
      }

      SendMsg2Socket(fd_progress,
        StringifyInt(cvmfs::directory_handles_->size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBuffer) {
      SendMsg2Socket(fd_progress, "Migrating inode tracker (v1 to v4)... ");
      compat::inode_tracker::InodeTracker *saved_inode_tracker =
        (compat::inode_tracker::InodeTracker *)saved_states[i]->state;
      compat::inode_tracker::Migrate(
        saved_inode_tracker, cvmfs::inode_tracker_);
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV2) {
      SendMsg2Socket(fd_progress, "Migrating inode tracker (v2 to v4)... ");
      compat::inode_tracker_v2::InodeTracker *saved_inode_tracker =
        (compat::inode_tracker_v2::InodeTracker *)saved_states[i]->state;
      compat::inode_tracker_v2::Migrate(saved_inode_tracker,
                                        cvmfs::inode_tracker_);
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV3) {
      SendMsg2Socket(fd_progress, "Migrating inode tracker (v3 to v4)... ");
      compat::inode_tracker_v3::InodeTracker *saved_inode_tracker =
        (compat::inode_tracker_v3::InodeTracker *)saved_states[i]->state;
      compat::inode_tracker_v3::Migrate(saved_inode_tracker,
                                        cvmfs::inode_tracker_);
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV4) {
      SendMsg2Socket(fd_progress, "Restoring inode tracker... ");
      delete cvmfs::inode_tracker_;
      glue::InodeTracker *saved_inode_tracker =
        (glue::InodeTracker *)saved_states[i]->state;
      cvmfs::inode_tracker_ = new glue::InodeTracker(*saved_inode_tracker);
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFiles) {
      SendMsg2Socket(fd_progress, "Migrating chunk tables (v1 to v3)... ");
      compat::chunk_tables::ChunkTables *saved_chunk_tables =
        (compat::chunk_tables::ChunkTables *)saved_states[i]->state;
      compat::chunk_tables::Migrate(saved_chunk_tables, cvmfs::chunk_tables_);
      SendMsg2Socket(fd_progress,
        StringifyInt(cvmfs::chunk_tables_->handle2fd.size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesV2) {
      SendMsg2Socket(fd_progress, "Migrating chunk tables (v2 to v3)... ");
      compat::chunk_tables_v2::ChunkTables *saved_chunk_tables =
        (compat::chunk_tables_v2::ChunkTables *)saved_states[i]->state;
      compat::chunk_tables_v2::Migrate(saved_chunk_tables,
                                       cvmfs::chunk_tables_);
      SendMsg2Socket(fd_progress,
        StringifyInt(cvmfs::chunk_tables_->handle2fd.size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesV3) {
      SendMsg2Socket(fd_progress, "Restoring chunk tables... ");
      delete cvmfs::chunk_tables_;
      ChunkTables *saved_chunk_tables = reinterpret_cast<ChunkTables *>(
        saved_states[i]->state);
      cvmfs::chunk_tables_ = new ChunkTables(*saved_chunk_tables);
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateInodeGeneration) {
      SendMsg2Socket(fd_progress, "Restoring inode generation... ");
      cvmfs::InodeGenerationInfo *old_info =
        (cvmfs::InodeGenerationInfo *)saved_states[i]->state;
      if (old_info->version == 1) {
        // Migration
        cvmfs::inode_generation_info_.initial_revision =
          old_info->initial_revision;
        cvmfs::inode_generation_info_.incarnation = old_info->incarnation;
        // Note: in the rare case of inode generation being 0 before, inode
        // can clash after reload before remount
      } else {
        cvmfs::inode_generation_info_ = *old_info;
      }
      ++cvmfs::inode_generation_info_.incarnation;
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesCounter) {
      SendMsg2Socket(fd_progress, "Restoring open files counter... ");
      cvmfs::no_open_files_->Set(*(reinterpret_cast<uint32_t *>(
        saved_states[i]->state)));
      SendMsg2Socket(fd_progress, " done\n");
    }
  }
  if (cvmfs::inode_annotation_) {
    uint64_t saved_generation = cvmfs::inode_generation_info_.inode_generation;
    cvmfs::inode_annotation_->IncGeneration(saved_generation);
  }

  return true;
}


static void FreeSavedState(const int fd_progress,
                           const loader::StateList &saved_states)
{
  for (unsigned i = 0, l = saved_states.size(); i < l; ++i) {
    switch (saved_states[i]->state_id) {
      case loader::kStateOpenDirs:
        SendMsg2Socket(fd_progress, "Releasing saved open directory handles\n");
        delete static_cast<cvmfs::DirectoryHandles *>(saved_states[i]->state);
        break;
      case loader::kStateGlueBuffer:
        SendMsg2Socket(
          fd_progress, "Releasing saved glue buffer (version 1)\n");
        delete static_cast<compat::inode_tracker::InodeTracker *>(
          saved_states[i]->state);
        break;
      case loader::kStateGlueBufferV2:
        SendMsg2Socket(
          fd_progress, "Releasing saved glue buffer (version 2)\n");
        delete static_cast<compat::inode_tracker_v2::InodeTracker *>(
          saved_states[i]->state);
        break;
      case loader::kStateGlueBufferV3:
        SendMsg2Socket(
          fd_progress, "Releasing saved glue buffer (version 3)\n");
        delete static_cast<compat::inode_tracker_v3::InodeTracker *>(
          saved_states[i]->state);
        break;
      case loader::kStateGlueBufferV4:
        SendMsg2Socket(fd_progress, "Releasing saved glue buffer\n");
        delete static_cast<glue::InodeTracker *>(saved_states[i]->state);
        break;
      case loader::kStateOpenFiles:
        SendMsg2Socket(fd_progress, "Releasing chunk tables (version 1)\n");
        delete static_cast<compat::chunk_tables::ChunkTables *>(
          saved_states[i]->state);
        break;
      case loader::kStateOpenFilesV2:
        SendMsg2Socket(fd_progress, "Releasing chunk tables (version 2)\n");
        delete static_cast<compat::chunk_tables_v2::ChunkTables *>(
          saved_states[i]->state);
        break;
      case loader::kStateOpenFilesV3:
        SendMsg2Socket(fd_progress, "Releasing chunk tables\n");
        delete static_cast<ChunkTables *>(saved_states[i]->state);
        break;
      case loader::kStateInodeGeneration:
        SendMsg2Socket(fd_progress, "Releasing saved inode generation info\n");
        delete static_cast<cvmfs::InodeGenerationInfo *>(
          saved_states[i]->state);
        break;
      case loader::kStateOpenFilesCounter:
        SendMsg2Socket(fd_progress, "Releasing open files counter\n");
        delete static_cast<uint32_t *>(saved_states[i]->state);
        break;
      default:
        break;
    }
  }
}


static void __attribute__((constructor)) LibraryMain() {
  g_cvmfs_exports = new loader::CvmfsExports();
  g_cvmfs_exports->so_version = PACKAGE_VERSION;
  g_cvmfs_exports->fnAltProcessFlavor = AltProcessFlavor;
  g_cvmfs_exports->fnInit = Init;
  g_cvmfs_exports->fnSpawn = Spawn;
  g_cvmfs_exports->fnFini = Fini;
  g_cvmfs_exports->fnGetErrorMsg = GetErrorMsg;
  g_cvmfs_exports->fnMaintenanceMode = MaintenanceMode;
  g_cvmfs_exports->fnSaveState = SaveState;
  g_cvmfs_exports->fnRestoreState = RestoreState;
  g_cvmfs_exports->fnFreeSavedState = FreeSavedState;
  cvmfs::SetCvmfsOperations(&g_cvmfs_exports->cvmfs_operations);
}


static void __attribute__((destructor)) LibraryExit() {
  delete g_cvmfs_exports;
  g_cvmfs_exports = NULL;
}
