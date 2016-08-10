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
#include "authz/authz_session.h"
#include "auto_umount.h"
#include "backoff.h"
#include "cache.h"
#include "catalog_mgr_client.h"
#include "clientctx.h"
#include "compat.h"
#include "compression.h"
#include "directory_entry.h"
#include "download.h"
#include "fence.h"
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
#include "mountpoint.h"
#include "nfs_maps.h"
#include "options.h"
#include "platform.h"
#include "quota.h"
#include "quota_listener.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "sqlitemem.h"
#include "sqlitevfs.h"
#include "statistics.h"
#include "talk.h"
#include "tracer.h"
#include "util_concurrency.h"
#include "uuid.h"
#include "wpad.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace cvmfs {

const unsigned kReloadSafetyMargin = 500;  // in milliseconds

FileSystem *file_system_ = NULL;
MountPoint *mount_point_ = NULL;
TalkManager *talk_mgr_ = NULL;

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
OptionsManager *options_mgr_ = NULL;
pid_t pid_ = 0;  /**< will be set after deamon() */
quota::ListenerHandle *watchdog_listener_ = NULL;
quota::ListenerHandle *unpin_listener_ = NULL;


/**
 * in maintenance mode, cache timeout is 0 and catalogs are not reloaded
 */
atomic_int32 maintenance_mode_;
atomic_int32 catalogs_expired_;
atomic_int32 drainout_mode_;
atomic_int32 reload_critical_section_;
time_t drainout_deadline_;
time_t catalogs_valid_until_;

typedef google::dense_hash_map<uint64_t, DirectoryListing,
                               hash_murmur<uint64_t> >
        DirectoryHandles;
DirectoryHandles *directory_handles_ = NULL;
pthread_mutex_t lock_directory_handles_ = PTHREAD_MUTEX_INITIALIZER;
uint64_t next_directory_handle_ = 0;

unsigned max_open_files_; /**< maximum allowed number of open files */
/**
 * Number of reserved file descriptors for internal use
 */
const int kNumReservedFd = 512;

/**
 * Ensures that within a callback all operations take place on the same
 * catalog revision.
 */
Fence *fence_remount_;


/**
 * The thread that triggers the reload of the root catalog is informed through
 * this pipe by the alarm signal handler when the TTL expires.
 */
int pipe_remount_trigger_[2];

/**
 * Triggers `RemountCheck()` when the repository TTL expires.
 */
pthread_t *thread_remount_trigger_ = NULL;


static inline double GetKcacheTimeout() {
  if (atomic_read32(&drainout_mode_) || atomic_read32(&maintenance_mode_))
    return 0.0;
  return mount_point_->kcache_timeout_sec();
}


void GetReloadStatus(bool *drainout_mode, bool *maintenance_mode) {
  *drainout_mode = atomic_read32(&drainout_mode_);
  *maintenance_mode = atomic_read32(&maintenance_mode_);
}


static bool UseWatchdog() {
  if (loader_exports_ == NULL || loader_exports_->version < 2) {
    return true;  // spawn watchdog by default
                  // Note: with library versions before 2.1.8 it might not
                  //       create stack traces properly in all cases
  }

  return !loader_exports_->disable_watchdog;
}


std::string PrintInodeGeneration() {
  return "init-catalog-revision: " +
    StringifyInt(inode_generation_info_.initial_revision) + "  " +
    "current-catalog-revision: " +
    StringifyInt(mount_point_->catalog_mgr()->GetRevision()) + "  " +
    "incarnation: " + StringifyInt(inode_generation_info_.incarnation) + "  " +
    "inode generation: " + StringifyInt(inode_generation_info_.inode_generation)
    + "\n";
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
  catalog::LoadError retval = mount_point_->catalog_mgr()->Remount(true);
  if (retval == catalog::kLoadNew) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "new catalog revision available, draining out meta-data caches");
    unsigned safety_margin = kReloadSafetyMargin/1000;
    if (safety_margin == 0)
      safety_margin = 1;
    drainout_deadline_ =
      time(NULL) +
      static_cast<int>(mount_point_->kcache_timeout_sec()) +
      safety_margin;
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
    mount_point_->inode_cache()->Pause();
    mount_point_->path_cache()->Pause();
    mount_point_->md5path_cache()->Pause();
    mount_point_->inode_cache()->Drop();
    mount_point_->path_cache()->Drop();
    mount_point_->md5path_cache()->Drop();

    // Ensure that all Fuse callbacks left the catalog query code
    fence_remount_->Drain();
    catalog::LoadError retval = mount_point_->catalog_mgr()->Remount(false);
    if (mount_point_->inode_annotation()) {
      inode_generation_info_.inode_generation =
        mount_point_->inode_annotation()->GetGeneration();
    }
    mount_point_->ReEvaluateAuthz();
    fence_remount_->Open();

    mount_point_->inode_cache()->Resume();
    mount_point_->path_cache()->Resume();
    mount_point_->md5path_cache()->Resume();

    atomic_cas32(&drainout_mode_, 1, 0);
    if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace) ||
        mount_point_->catalog_mgr()->offline_mode())
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "reload/finish failed, "
               "applying short term TTL");
      alarm(MountPoint::kShortTermTTL);
      catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
    } else {
      LogCvmfs(kLogCvmfs, kLogSyslog, "switched to catalog revision %d",
               mount_point_->catalog_mgr()->GetRevision());
      unsigned effective_ttl = mount_point_->GetEffectiveTtlSec();
      alarm(effective_ttl);
      catalogs_valid_until_ = time(NULL) + effective_ttl;
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
      alarm(MountPoint::kShortTermTTL);
      catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
    } else if (retval == catalog::kLoadUp2Date) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "catalog up to date, applying effective TTL");
      unsigned effective_ttl = mount_point_->GetEffectiveTtlSec();
      alarm(effective_ttl);
      catalogs_valid_until_ = time(NULL) + effective_ttl;
    }
  }
}


static bool CheckVoms(const fuse_ctx &fctx) {
  if (!mount_point_->has_membership_req())
    return true;
  string mreq = mount_point_->membership_req();
  LogCvmfs(kLogCvmfs, kLogDebug, "Got VOMS authz %s from filesystem "
           "properties", mreq.c_str());

  if (fctx.uid == 0)
    return true;

  return mount_point_->authz_session_mgr()->IsMemberOf(fctx.pid, mreq);
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
  if (mount_point_->inode_cache()->Lookup(ino, dirent))
    return true;

  // Look in the catalogs in 2 steps: lookup inode->path, lookup path
  catalog::DirectoryEntry dirent_negative =
    catalog::DirectoryEntry(catalog::kDirentNegative);
  // Reset directory entry.  If the function returns false and dirent is no
  // the kDirentNegative, it was an I/O error
  *dirent = catalog::DirectoryEntry();

  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();

  if (file_system_->IsNfsSource()) {
    // NFS mode
    PathString path;
    bool retval = nfs_maps::GetPath(ino, &path);
    if (!retval) {
      *dirent = dirent_negative;
      return false;
    }
    if (catalog_mgr->LookupPath(path, catalog::kLookupSole, dirent)) {
      // Fix inodes
      dirent->set_inode(ino);
      mount_point_->inode_cache()->Insert(ino, *dirent);
      return true;
    }
    return false;  // Not found in catalog or catalog load error
  }

  // Non-NFS mode
  PathString path;
  if (ino == catalog_mgr->GetRootInode()) {
    bool retval =
      catalog_mgr->LookupPath(PathString(), catalog::kLookupSole, dirent);
    assert(retval);
    dirent->set_inode(ino);
    mount_point_->inode_cache()->Insert(ino, *dirent);
    return true;
  }

  bool retval = mount_point_->inode_tracker()->FindPath(ino, &path);
  if (!retval) {
    // Can this ever happen?
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "GetDirentForInode inode lookup failure %" PRId64, ino);
    *dirent = dirent_negative;
    return false;
  }
  if (catalog_mgr->LookupPath(path, catalog::kLookupSole, dirent)) {
    // Fix inodes
    dirent->set_inode(ino);
    mount_point_->inode_cache()->Insert(ino, *dirent);
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
  if (!file_system_->IsNfsSource())
    live_inode = mount_point_->inode_tracker()->FindInode(path);

  shash::Md5 md5path(path.GetChars(), path.GetLength());
  if (mount_point_->md5path_cache()->Lookup(md5path, dirent)) {
    if (dirent->GetSpecial() == catalog::kDirentNegative)
      return false;
    if (!file_system_->IsNfsSource() && (live_inode != 0))
      dirent->set_inode(live_inode);
    return true;
  }

  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();

  // Lookup inode in catalog TODO: not twice md5 calculation
  bool retval;
  retval = catalog_mgr->LookupPath(path, catalog::kLookupSole, dirent);
  if (retval) {
    if (file_system_->IsNfsSource()) {
      // Fix inode
      dirent->set_inode(nfs_maps::GetInode(path));
    } else {
      if (live_inode != 0)
        dirent->set_inode(live_inode);
    }
    mount_point_->md5path_cache()->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  // Only insert ENOENT results into negative cache.  Otherwise it was an
  // error loading nested catalogs
  if (dirent->GetSpecial() == catalog::kDirentNegative)
    mount_point_->md5path_cache()->InsertNegative(md5path);
  return false;
}


static bool GetPathForInode(const fuse_ino_t ino, PathString *path) {
  // Check the path cache first
  if (mount_point_->path_cache()->Lookup(ino, path))
    return true;

  if (file_system_->IsNfsSource()) {
    // NFS mode, just a lookup
    LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - lookup in NFS maps", ino);
    if (nfs_maps::GetPath(ino, path)) {
      mount_point_->path_cache()->Insert(ino, *path);
      return true;
    }
    return false;
  }

  if (ino == mount_point_->catalog_mgr()->GetRootInode())
    return true;

  LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - looking in inode tracker", ino);
  bool retval = mount_point_->inode_tracker()->FindPath(ino, path);
  assert(retval);
  mount_point_->path_cache()->Insert(ino, *path);
  return true;
}


/**
 * Find the inode number of a file name in a directory given by inode.
 * This or getattr is called as kind of prerequisit to every operation.
 * We do check catalog TTL here (and reload, if necessary).
 */
static void cvmfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
  perf::Inc(file_system_->n_fs_lookup());
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  RemountCheck();

  fence_remount_->Enter();
  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();

  parent = catalog_mgr->MangleInode(parent);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_lookup in parent inode: %" PRIu64 " for name: %s",
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
        if (dirent.inode() == catalog_mgr->GetRootInode()) {
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
  mount_point_->tracer()->Trace(Tracer::kEventLookup, path, "lookup()");
  if (!GetDirentForPath(path, &dirent)) {
    if (dirent.GetSpecial() == catalog::kDirentNegative)
      goto lookup_reply_negative;
    else
      goto lookup_reply_error;
  }

 lookup_reply_positive:
  if (!file_system_->IsNfsSource())
    mount_point_->inode_tracker()->VfsGet(dirent.inode(), path);
  fence_remount_->Leave();
  result.ino = dirent.inode();
  result.attr = dirent.GetStatStructure();
  fuse_reply_entry(req, &result);
  return;

 lookup_reply_negative:
  fence_remount_->Leave();
  perf::Inc(file_system_->n_fs_lookup_negative());
  result.ino = 0;
  fuse_reply_entry(req, &result);
  return;

 lookup_reply_error:
  fence_remount_->Leave();
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
  perf::Inc(file_system_->n_fs_forget());

  // The libfuse high-level library does the same
  if (ino == FUSE_ROOT_ID) {
    fuse_reply_none(req);
    return;
  }

  fence_remount_->Enter();
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  // This has been seen to deadlock on the debug log mutex on SL5.  Problem of
  // old kernel/fuse?
  LogCvmfs(kLogCvmfs, kLogDebug, "forget on inode %" PRIu64 " by %u",
           uint64_t(ino), nlookup);
  if (!file_system_->IsNfsSource())
    mount_point_->inode_tracker()->VfsPut(ino, nlookup);
  fence_remount_->Leave();
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
  perf::Inc(file_system_->n_fs_stat());
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);
  RemountCheck();

  fence_remount_->Enter();
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_getattr (stat) for inode: %" PRIu64,
           uint64_t(ino));

  if (!CheckVoms(*fuse_ctx)) {
    fence_remount_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);
  fence_remount_->Leave();

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
  perf::Inc(file_system_->n_fs_readlink());
  const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
  ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

  fence_remount_->Enter();
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_readlink on inode: %" PRIu64,
           uint64_t(ino));

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);
  fence_remount_->Leave();

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
  LogCvmfs(kLogCvmfs, kLogDebug, "Add to listing: %s, inode %" PRIu64,
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

  fence_remount_->Enter();
  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();
  ino = catalog_mgr->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %" PRIu64,
           uint64_t(ino));

  if (!CheckVoms(*fuse_ctx)) {
    fence_remount_->Leave();
    fuse_reply_err(req, EACCES);
    return;
  }

  PathString path;
  catalog::DirectoryEntry d;
  bool found = GetPathForInode(ino, &path);
  if (!found) {
    fence_remount_->Leave();
    fuse_reply_err(req, ENOENT);
    return;
  }
  found = GetDirentForInode(ino, &d);

  if (!found) {
    fence_remount_->Leave();
    ReplyNegative(d, req);
    return;
  }
  if (!d.IsDirectory()) {
    fence_remount_->Leave();
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %" PRIu64 ", path %s",
           uint64_t(ino), path.c_str());

  // Build listing
  BigVector<char> fuse_listing(512);

  // Add current directory link
  struct stat info;
  info = d.GetStatStructure();
  AddToDirListing(req, ".", &info, &fuse_listing);

  // Add parent directory link
  catalog::DirectoryEntry p;
  if (d.inode() != catalog_mgr->GetRootInode() &&
      GetDirentForPath(GetParentPath(path), &p))
  {
    info = p.GetStatStructure();
    AddToDirListing(req, "..", &info, &fuse_listing);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  bool retval = catalog_mgr->ListingStat(path, &listing_from_catalog);

  if (!retval) {
    fence_remount_->Leave();
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
  fence_remount_->Leave();

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
           "linking directory handle %d to dir inode: %" PRIu64,
           next_directory_handle_, uint64_t(ino));
  (*directory_handles_)[next_directory_handle_] = stream_listing;
  fi->fh = next_directory_handle_;
  ++next_directory_handle_;
  pthread_mutex_unlock(&lock_directory_handles_);
  perf::Inc(file_system_->n_fs_dir_open());
  perf::Inc(file_system_->no_open_dirs());

  fuse_reply_open(req, fi);
}


/**
 * Release a directory.
 */
static void cvmfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi)
{
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_releasedir on inode %" PRIu64
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
    perf::Dec(file_system_->no_open_dirs());
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
           "cvmfs_readdir on inode %" PRIu64 " reading %d bytes from offset %d",
           uint64_t(mount_point_->catalog_mgr()->MangleInode(ino)), size, off);

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
  fence_remount_->Enter();
  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();
  ino = catalog_mgr->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on inode: %" PRIu64,
           uint64_t(ino));

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;

  bool found = GetPathForInode(ino, &path);
  if (!found) {
    fence_remount_->Leave();
    fuse_reply_err(req, ENOENT);
    return;
  }
  found = GetDirentForInode(ino, &dirent);
  if (!found) {
    fence_remount_->Leave();
    ReplyNegative(dirent, req);
    return;
  }

  if (!CheckVoms(*fuse_ctx)) {
    fence_remount_->Leave();
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
    fence_remount_->Leave();
    fuse_reply_err(req, EOPNOTSUPP);
    return;
  }
#endif
  if (fi->flags & O_EXCL) {
    fence_remount_->Leave();
    fuse_reply_err(req, EEXIST);
    return;
  }

  perf::Inc(file_system_->n_fs_open());  // Count actual open / fetch operations

  if (!dirent.IsChunkedFile()) {
    fence_remount_->Leave();
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "chunked file %s opened (download delayed to read() call)",
             path.c_str());

    if (perf::Xadd(file_system_->no_open_files(), 1) >=
        (static_cast<int>(max_open_files_))-kNumReservedFd)
    {
      perf::Dec(file_system_->no_open_files());
      fence_remount_->Leave();
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "open file descriptor limit exceeded");
      fuse_reply_err(req, EMFILE);
      return;
    }

    // Figure out unique inode from annotated catalog
    catalog::DirectoryEntry dirent_origin;
    if (!catalog_mgr->LookupPath(path, catalog::kLookupSole, &dirent_origin)) {
      fence_remount_->Leave();
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "chunked file %s vanished unexpectedly", path.c_str());
      fuse_reply_err(req, ENOENT);
      return;
    }
    const uint64_t unique_inode = dirent_origin.inode();

    ChunkTables *chunk_tables = mount_point_->chunk_tables();
    chunk_tables->Lock();
    if (!chunk_tables->inode2chunks.Contains(unique_inode)) {
      chunk_tables->Unlock();

      // Retrieve File chunks from the catalog
      UniquePtr<FileChunkList> chunks(new FileChunkList());
      if (!catalog_mgr->ListFileChunks(path, dirent.hash_algorithm(),
                                       chunks.weak_ref()) ||
          chunks->IsEmpty())
      {
        fence_remount_->Leave();
        LogCvmfs(kLogCvmfs, kLogDebug| kLogSyslogErr, "file %s is marked as "
                 "'chunked', but no chunks found.", path.c_str());
        fuse_reply_err(req, EIO);
        return;
      }
      fence_remount_->Leave();

      chunk_tables->Lock();
      // Check again to avoid race
      if (!chunk_tables->inode2chunks.Contains(unique_inode)) {
        chunk_tables->inode2chunks.Insert(
          unique_inode, FileChunkReflist(chunks.Release(), path,
                                         dirent.compression_algorithm(),
                                         dirent.IsExternalFile()));
        chunk_tables->inode2references.Insert(unique_inode, 1);
      } else {
        uint32_t refctr;
        bool retval =
          chunk_tables->inode2references.Lookup(unique_inode, &refctr);
        assert(retval);
        chunk_tables->inode2references.Insert(unique_inode, refctr+1);
      }
    } else {
      fence_remount_->Leave();
      uint32_t refctr;
      bool retval =
        chunk_tables->inode2references.Lookup(unique_inode, &refctr);
      assert(retval);
      chunk_tables->inode2references.Insert(unique_inode, refctr+1);
    }

    // Update the chunk handle list
    LogCvmfs(kLogCvmfs, kLogDebug,
             "linking chunk handle %d to unique inode: %" PRIu64,
             chunk_tables->next_handle, uint64_t(unique_inode));
    chunk_tables->handle2fd.Insert(chunk_tables->next_handle, ChunkFd());
    chunk_tables->handle2uniqino.Insert(chunk_tables->next_handle,
                                        unique_inode);
    // The same inode can refer to different revisions of a path.  Don't cache.
    fi->keep_cache = 0;
    fi->fh = static_cast<uint64_t>(-chunk_tables->next_handle);
    ++chunk_tables->next_handle;
    chunk_tables->Unlock();

    fuse_reply_open(req, fi);
    return;
  }

  Fetcher *this_fetcher = dirent.IsExternalFile()
    ? mount_point_->external_fetcher()
    : mount_point_->fetcher();
  fd = this_fetcher->Fetch(
    dirent.checksum(),
    dirent.size(),
    string(path.GetChars(), path.GetLength()),
    dirent.compression_algorithm(),
    mount_point_->catalog_mgr()->volatile_flag()
      ? cache::CacheManager::kTypeVolatile
      : cache::CacheManager::kTypeRegular);

  if (fd >= 0) {
    if (perf::Xadd(file_system_->no_open_files(), 1) <
        (static_cast<int>(max_open_files_))-kNumReservedFd) {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
      // The same inode can refer to different revisions of a path. Don't cache.
      fi->keep_cache = 0;
      fi->fh = fd;
      fuse_reply_open(req, fi);
      return;
    } else {
      if (file_system_->cache_mgr()->Close(fd) == 0)
        perf::Dec(file_system_->no_open_files());
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "open file descriptor limit exceeded");
      fuse_reply_err(req, EMFILE);
      return;
    }
    assert(false);
  }

  // fd < 0
  LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
           "failed to open inode: %" PRIu64 ", CAS key %s, error code %d",
           uint64_t(ino), dirent.checksum().ToString().c_str(), errno);
  if (errno == EMFILE) {
    fuse_reply_err(req, EMFILE);
    return;
  }

  mount_point_->backoff_throttle()->Throttle();

  perf::Inc(file_system_->n_io_error());
  fuse_reply_err(req, -fd);
}


/**
 * Redirected to pread into cache.
 */
static void cvmfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_read inode: %" PRIu64 " reading %d bytes from offset %d "
           "fd %d", uint64_t(mount_point_->catalog_mgr()->MangleInode(ino)),
           size, off, fi->fh);
  perf::Inc(file_system_->n_fs_read());

  // Get data chunk (<=128k guaranteed by Fuse)
  char *data = static_cast<char *>(alloca(size));
  unsigned int overall_bytes_fetched = 0;

  // Do we have a a chunked file?
  if (static_cast<int64_t>(fi->fh) < 0) {
    const struct fuse_ctx *fuse_ctx = fuse_req_ctx(req);
    ClientCtxGuard ctx_guard(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid);

    const uint64_t chunk_handle =
      static_cast<uint64_t>(-static_cast<int64_t>(fi->fh));
    uint64_t unique_inode;
    ChunkFd chunk_fd;
    FileChunkReflist chunks;
    bool retval;

    // Fetch unique inode, chunk list and file descriptor
    ChunkTables *chunk_tables = mount_point_->chunk_tables();
    chunk_tables->Lock();
    retval = chunk_tables->handle2uniqino.Lookup(chunk_handle, &unique_inode);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogDebug, "no unique inode, fall back to fuse ino");
      unique_inode = ino;
    }
    retval = chunk_tables->inode2chunks.Lookup(unique_inode, &chunks);
    assert(retval);
    chunk_tables->Unlock();

    unsigned chunk_idx = chunks.FindChunkIdx(off);

    // Lock chunk handle
    pthread_mutex_t *handle_lock = chunk_tables->Handle2Lock(chunk_handle);
    LockMutex(handle_lock);
    chunk_tables->Lock();
    retval = chunk_tables->handle2fd.Lookup(chunk_handle, &chunk_fd);
    assert(retval);
    chunk_tables->Unlock();

    // Fetch all needed chunks and read the requested data
    off_t offset_in_chunk = off - chunks.list->AtPtr(chunk_idx)->offset();
    do {
      // Open file descriptor to chunk
      if ((chunk_fd.fd == -1) || (chunk_fd.chunk_idx != chunk_idx)) {
        if (chunk_fd.fd != -1) file_system_->cache_mgr()->Close(chunk_fd.fd);
        string verbose_path = "Part of " + chunks.path.ToString();
        if (chunks.external_data) {
          chunk_fd.fd = mount_point_->external_fetcher()->Fetch(
            chunks.list->AtPtr(chunk_idx)->content_hash(),
            chunks.list->AtPtr(chunk_idx)->size(),
            verbose_path,
            chunks.compression_alg,
            mount_point_->catalog_mgr()->volatile_flag()
              ? cache::CacheManager::kTypeVolatile
              : cache::CacheManager::kTypeRegular,
            chunks.path.ToString(),
            chunks.list->AtPtr(chunk_idx)->offset());
        } else {
          chunk_fd.fd = mount_point_->fetcher()->Fetch(
            chunks.list->AtPtr(chunk_idx)->content_hash(),
            chunks.list->AtPtr(chunk_idx)->size(),
            verbose_path,
            chunks.compression_alg,
            mount_point_->catalog_mgr()->volatile_flag()
              ? cache::CacheManager::kTypeVolatile
              : cache::CacheManager::kTypeRegular);
        }
        if (chunk_fd.fd < 0) {
          chunk_fd.fd = -1;
          chunk_tables->Lock();
          chunk_tables->handle2fd.Insert(chunk_handle, chunk_fd);
          chunk_tables->Unlock();
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
      const int64_t bytes_fetched = file_system_->cache_mgr()->Pread(
        chunk_fd.fd,
        data + overall_bytes_fetched,
        bytes_to_read_in_chunk,
        offset_in_chunk);

      if (bytes_fetched < 0) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr, "read err no %" PRId64 " (%s)",
                 bytes_fetched, chunks.path.ToString().c_str());
        chunk_tables->Lock();
        chunk_tables->handle2fd.Insert(chunk_handle, chunk_fd);
        chunk_tables->Unlock();
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
    chunk_tables->Lock();
    chunk_tables->handle2fd.Insert(chunk_handle, chunk_fd);
    chunk_tables->Unlock();
    UnlockMutex(handle_lock);
    LogCvmfs(kLogCvmfs, kLogDebug, "released chunk file descriptor %d",
             chunk_fd.fd);
  } else {
    const int64_t fd = fi->fh;
    int64_t nbytes = file_system_->cache_mgr()->Pread(fd, data, size, off);
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
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_release on inode: %" PRIu64,
           uint64_t(ino));
  const int64_t fd = fi->fh;

  // do we have a chunked file?
  if (static_cast<int64_t>(fi->fh) < 0) {
    const uint64_t chunk_handle =
      static_cast<uint64_t>(-static_cast<int64_t>(fi->fh));
    LogCvmfs(kLogCvmfs, kLogDebug, "releasing chunk handle %" PRIu64,
             chunk_handle);
    uint64_t unique_inode;
    ChunkFd chunk_fd;
    FileChunkReflist chunks;
    uint32_t refctr;
    bool retval;

    ChunkTables *chunk_tables = mount_point_->chunk_tables();
    chunk_tables->Lock();
    retval = chunk_tables->handle2uniqino.Lookup(chunk_handle, &unique_inode);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogDebug, "no unique inode, fall back to fuse ino");
      unique_inode = ino;
    } else {
      chunk_tables->handle2uniqino.Erase(chunk_handle);
    }
    retval = chunk_tables->handle2fd.Lookup(chunk_handle, &chunk_fd);
    assert(retval);
    chunk_tables->handle2fd.Erase(chunk_handle);

    retval = chunk_tables->inode2references.Lookup(unique_inode, &refctr);
    assert(retval);
    refctr--;
    if (refctr == 0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "releasing chunk list for inode %" PRIu64,
               uint64_t(unique_inode));
      FileChunkReflist to_delete;
      retval = chunk_tables->inode2chunks.Lookup(unique_inode, &to_delete);
      assert(retval);
      chunk_tables->inode2references.Erase(unique_inode);
      chunk_tables->inode2chunks.Erase(unique_inode);
      delete to_delete.list;
    } else {
      chunk_tables->inode2references.Insert(unique_inode, refctr);
    }
    chunk_tables->Unlock();

    if (chunk_fd.fd != -1)
      file_system_->cache_mgr()->Close(chunk_fd.fd);
    perf::Dec(file_system_->no_open_files());
  } else {
    if (file_system_->cache_mgr()->Close(fd) == 0) {
      perf::Dec(file_system_->no_open_files());
    }
  }
  fuse_reply_err(req, 0);
}


static void cvmfs_statfs(fuse_req_t req, fuse_ino_t ino) {
  ino = mount_point_->catalog_mgr()->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_statfs on inode: %" PRIu64,
           uint64_t(ino));

  // If we return 0 it will cause the fs to be ignored in "df"
  struct statvfs info;
  memset(&info, 0, sizeof(info));

  // Unmanaged cache
  if (!file_system_->cache_mgr()->quota_mgr()->IsEnforcing()) {
    fuse_reply_statfs(req, &info);
    return;
  }

  uint64_t available = 0;
  uint64_t size = file_system_->cache_mgr()->quota_mgr()->GetSize();
  uint64_t capacity = file_system_->cache_mgr()->quota_mgr()->GetCapacity();
  info.f_bsize = 1;

  if (capacity == (uint64_t)(-1)) {
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
    info.f_blocks = capacity;
    available = capacity - size;
  }

  info.f_bfree = info.f_bavail = available;

  // Inodes / entries
  fence_remount_->Enter();
  uint64_t all_inodes = mount_point_->catalog_mgr()->all_inodes();
  uint64_t loaded_inode = mount_point_->catalog_mgr()->loaded_inodes();
  info.f_files = all_inodes;
  info.f_ffree = info.f_favail = all_inodes - loaded_inode;
  fence_remount_->Leave();

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

  fence_remount_->Enter();
  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();
  ino = catalog_mgr->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_getxattr on inode: %" PRIu64 " for xattr: %s",
           uint64_t(ino), name);

  if (!CheckVoms(*fuse_ctx)) {
    fence_remount_->Leave();
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
    retval = catalog_mgr->LookupXattrs(path, &xattrs);
    assert(retval);
  }
  if (d.IsLink()) {
    PathString path;
    catalog::LookupOptions lookup_options = static_cast<catalog::LookupOptions>(
      catalog::kLookupSole | catalog::kLookupRawSymlink);
    catalog::DirectoryEntry raw_symlink;
    retval = catalog_mgr->LookupPath(path, lookup_options, &raw_symlink);
    assert(retval);
    d.set_symlink(raw_symlink.symlink());
  }
  fence_remount_->Leave();

  if (!found) {
    ReplyNegative(d, req);
    return;
  }

  string attribute_value;

  if (attr == "user.pid") {
    attribute_value = StringifyInt(pid_);
  } else if (attr == "user.version") {
    attribute_value = string(VERSION) + "." + string(CVMFS_PATCH_LEVEL);
  } else if (attr == "user.pubkeys") {
    attribute_value = mount_point_->signature_mgr()->GetActivePubkeys();
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
      int fd = file_system_->cache_mgr()->Open(d.checksum());
      if (fd < 0) {
        attribute_value = "Not in cache";
      } else {
        shash::Any hash(d.checksum().algorithm);
        int retval_i = file_system_->cache_mgr()->ChecksumFd(fd, &hash);
        if (retval_i != 0)
          attribute_value = "I/O error (" + StringifyInt(retval_i) + ")";
        else
          attribute_value = hash.ToString();
        file_system_->cache_mgr()->Close(fd);
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
    const uint64_t revision = catalog_mgr->GetRevision();
    attribute_value = StringifyInt(revision);
  } else if (attr == "user.root_hash") {
    attribute_value = catalog_mgr->GetRootHash().ToString();
  } else if (attr == "user.tag") {
    attribute_value = mount_point_->repository_tag();
  } else if (attr == "user.expires") {
    if (catalogs_valid_until_ == MountPoint::kIndefiniteDeadline) {
      attribute_value = "never (fixed root catalog)";
    } else {
      time_t now = time(NULL);
      attribute_value = StringifyInt((catalogs_valid_until_-now)/60);
    }
  } else if (attr == "user.maxfd") {
    attribute_value = StringifyInt(max_open_files_ - kNumReservedFd);
  } else if (attr == "user.usedfd") {
    attribute_value = file_system_->no_open_files()->ToString();
  } else if (attr == "user.useddirp") {
    attribute_value = file_system_->no_open_dirs()->ToString();
  } else if (attr == "user.nioerr") {
    attribute_value = file_system_->n_io_error()->ToString();
  } else if (attr == "user.proxy") {
    vector< vector<download::DownloadManager::ProxyInfo> > proxy_chain;
    unsigned current_group;
    mount_point_->download_mgr()->GetProxyInfo(
      &proxy_chain, &current_group, NULL);
    if (proxy_chain.size()) {
      attribute_value = proxy_chain[current_group][0].url;
    } else {
      attribute_value = "DIRECT";
    }
  } else if (attr == "user.authz") {
    if (!mount_point_->has_membership_req()) {
      fuse_reply_err(req, ENOATTR);
      return;
    }
    attribute_value = mount_point_->membership_req();
  } else if (attr == "user.chunks") {
    if (d.IsRegular()) {
      if (d.IsChunkedFile()) {
        PathString path;
        retval = GetPathForInode(ino, &path);
        assert(retval);

        FileChunkList chunks;
        if (!catalog_mgr->ListFileChunks(path, d.hash_algorithm(), &chunks) ||
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
    mount_point_->external_download_mgr()->GetHostInfo(
      &host_chain, &rtt, &current_host);
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
    mount_point_->download_mgr()->GetHostInfo(&host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      attribute_value = string(host_chain[current_host]);
    } else {
      attribute_value = "internal error: no hosts defined";
    }
  } else if (attr == "user.host_list") {
    vector<string> host_chain;
    vector<int> rtt;
    unsigned current_host;
    mount_point_->download_mgr()->GetHostInfo(&host_chain, &rtt, &current_host);
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
    uint64_t uptime = now - loader_exports_->boot_time;
    attribute_value = StringifyInt(uptime / 60);
  } else if (attr == "user.nclg") {
    const int num_catalogs = catalog_mgr->GetNumCatalogs();
    attribute_value = StringifyInt(num_catalogs);
  } else if (attr == "user.nopen") {
    attribute_value = file_system_->n_fs_open()->ToString();
  } else if (attr == "user.ndiropen") {
    attribute_value = file_system_->n_fs_dir_open()->ToString();
  } else if (attr == "user.ndownload") {
    attribute_value =
      mount_point_->statistics()->Lookup("fetch.n_downloads")->Print();
  } else if (attr == "user.timeout") {
    unsigned seconds, seconds_direct;
    mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds);
  } else if (attr == "user.timeout_direct") {
    unsigned seconds, seconds_direct;
    mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.external_timeout") {
    unsigned seconds, seconds_direct;
    mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.rx") {
    perf::Statistics *statistics = mount_point_->statistics();
    int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
    attribute_value = StringifyInt(rx/1024);
  } else if (attr == "user.speed") {
    perf::Statistics *statistics = mount_point_->statistics();
    int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
    int64_t time = statistics->Lookup("download.sz_transfer_time")->Get();
    if (time == 0)
      attribute_value = "n/a";
    else
      attribute_value = StringifyInt((rx/1024)/time);
  } else if (attr == "user.fqrn") {
    attribute_value = loader_exports_->repository_name;
  } else if (attr == "user.inode_max") {
    attribute_value = StringifyInt(
      inode_generation_info_.inode_generation +
      catalog_mgr->inode_gauge());
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

  fence_remount_->Enter();
  catalog::ClientCatalogManager *catalog_mgr = mount_point_->catalog_mgr();
  ino = catalog_mgr->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_listxattr on inode: %" PRIu64 ", size %u [hide xattrs %d]",
           uint64_t(ino), size, mount_point_->hide_magic_xattrs());

  catalog::DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);
  XattrList xattrs;
  if (d.HasXattrs()) {
    PathString path;
    bool retval = GetPathForInode(ino, &path);
    assert(retval);
    retval = catalog_mgr->LookupXattrs(path, &xattrs);
    assert(retval);
  }
  fence_remount_->Leave();

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
  if (mount_point_->hide_magic_xattrs()) {
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

    if (mount_point_->has_membership_req()) {
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
  fence_remount_->Enter();
  const bool found = GetDirentForPath(PathString(path), &dirent);
  fence_remount_->Leave();

  if (!found || !dirent.IsRegular())
    return false;
  file_system_->cache_mgr()->quota_mgr()->Remove(dirent.checksum());
  return true;
}


bool Pin(const string &path) {
  catalog::DirectoryEntry dirent;
  fence_remount_->Enter();
  const bool found = GetDirentForPath(PathString(path), &dirent);
  if (!found || !dirent.IsRegular()) {
    fence_remount_->Leave();
    return false;
  }

  if (!dirent.IsChunkedFile()) {
    fence_remount_->Leave();
  } else {
    FileChunkList chunks;
    mount_point_->catalog_mgr()->ListFileChunks(
      PathString(path), dirent.hash_algorithm(), &chunks);
    fence_remount_->Leave();
    for (unsigned i = 0; i < chunks.size(); ++i) {
      bool retval =
        file_system_->cache_mgr()->quota_mgr()->Pin(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          false);
      if (!retval)
        return false;
      int fd = -1;
      if (dirent.IsExternalFile()) {
        fd = mount_point_->external_fetcher()->Fetch(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          dirent.compression_algorithm(),
          cache::CacheManager::kTypePinned,
          path,
          chunks.AtPtr(i)->offset());
      } else {
        fd = mount_point_->fetcher()->Fetch(
          chunks.AtPtr(i)->content_hash(),
          chunks.AtPtr(i)->size(),
          "Part of " + path,
          dirent.compression_algorithm(),
          cache::CacheManager::kTypePinned);
      }
      if (fd < 0) {
        return false;
      }
      file_system_->cache_mgr()->Close(fd);
    }
    return true;
  }

  bool retval = file_system_->cache_mgr()->quota_mgr()->Pin(
    dirent.checksum(), dirent.size(), path, false);
  if (!retval)
    return false;
  Fetcher *this_fetcher = dirent.IsExternalFile()
    ? mount_point_->external_fetcher()
    : mount_point_->fetcher();
  int fd = this_fetcher->Fetch(
    dirent.checksum(), dirent.size(), path, dirent.compression_algorithm(),
    cache::CacheManager::kTypePinned);
  if (fd < 0) {
    return false;
  }
  file_system_->cache_mgr()->Close(fd);
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


bool g_monitor_ready = false;
string *g_boot_error = NULL;

__attribute__((visibility("default")))
loader::CvmfsExports *g_cvmfs_exports = NULL;


/**
 * Construct a file system but prevent hanging when already mounted.  That
 * means: at most one "system" mount of any given repository name.
 */
static FileSystem *InitSystemFs(
  const string &mount_path,
  const string &fqrn,
  FileSystem::FileSystemInfo fs_info)
{
  fs_info.wait_workspace = false;
  FileSystem *file_system = FileSystem::Create(fs_info);

  if (file_system->boot_status() == loader::kFailLockWorkspace) {
    string fqrn_from_xattr;
    int retval = platform_getxattr(mount_path, "user.fqrn", &fqrn_from_xattr);
    if (!retval) {
      // Cvmfs not mounted anymore, but another cvmfs process is still in
      // shutdown procedure.  Try again and wait for lock
      delete file_system;
      fs_info.wait_workspace = true;
      file_system = FileSystem::Create(fs_info);
    } else {
      if (fqrn_from_xattr == fqrn) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
                 "repository already mounted on %s", mount_path.c_str());
        file_system->set_boot_status(loader::kFailDoubleMount);
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
                 "CernVM-FS repository %s already mounted on %s",
                 fqrn.c_str(), mount_path.c_str());
        file_system->set_boot_status(loader::kFailOtherMount);
      }
    }
  }

  return file_system;
}


static void InitOptionsMgr(const loader::LoaderExports *loader_exports) {
  if (loader_exports->version >= 3 && loader_exports->simple_options_parsing) {
    cvmfs::options_mgr_ = new SimpleOptionsParser();
  } else {
    cvmfs::options_mgr_ = new BashOptionsManager();
  }

  if (loader_exports->config_files != "") {
    vector<string> tokens = SplitString(loader_exports->config_files, ':');
    for (unsigned i = 0, s = tokens.size(); i < s; ++i) {
      cvmfs::options_mgr_->ParsePath(tokens[i], false);
    }
  } else {
    cvmfs::options_mgr_->ParseDefault(loader_exports->repository_name);
  }
}


static int Init(const loader::LoaderExports *loader_exports) {
  g_boot_error = new string("unknown error");
  cvmfs::loader_exports_ = loader_exports;
  InitOptionsMgr(loader_exports);

  FileSystem::FileSystemInfo fs_info;
  fs_info.type = FileSystem::kFsFuse;
  fs_info.name = loader_exports->repository_name;
  fs_info.exe_path = loader_exports->program_name;
  fs_info.options_mgr = cvmfs::options_mgr_;
  fs_info.foreground = loader_exports->foreground;
  cvmfs::file_system_ = InitSystemFs(
    loader_exports->mount_point,
    loader_exports->repository_name,
    fs_info);
  if (!cvmfs::file_system_->IsValid()) {
    *g_boot_error = cvmfs::file_system_->boot_error();
    return cvmfs::file_system_->boot_status();
  }

  cvmfs::mount_point_ = MountPoint::Create(loader_exports->repository_name,
                                           cvmfs::file_system_);
  if (!cvmfs::mount_point_->IsValid()) {
    *g_boot_error = cvmfs::mount_point_->boot_error();
    return cvmfs::mount_point_->boot_status();
  }

  cvmfs::directory_handles_ = new cvmfs::DirectoryHandles();
  cvmfs::directory_handles_->set_empty_key((uint64_t)(-1));
  cvmfs::directory_handles_->set_deleted_key((uint64_t)(-2));

  LogCvmfs(kLogCvmfs, kLogDebug, "fuse inode size is %d bits",
           sizeof(fuse_ino_t) * 8);

  // Monitor, check for maximum number of open files
  if (cvmfs::UseWatchdog()) {
    if (!monitor::Init(".", loader_exports->repository_name, true)) {
      *g_boot_error = "failed to initialize watchdog.";
      return loader::kFailMonitor;
    }
    g_monitor_ready = true;
  }
  cvmfs::max_open_files_ = monitor::GetMaxOpenFiles();

  // Control & command interface
  cvmfs::talk_mgr_ = TalkManager::Create(
    "./cvmfs_io." + cvmfs::mount_point_->fqrn(),
    cvmfs::mount_point_);
  if (cvmfs::talk_mgr_ == NULL) {
    *g_boot_error = "failed to initialize talk socket (" +
                    StringifyInt(errno) + ")";
    return loader::kFailTalk;
  }

  cvmfs::inode_generation_info_.initial_revision =
    cvmfs::mount_point_->catalog_mgr()->GetRevision();
  cvmfs::inode_generation_info_.inode_generation =
    cvmfs::mount_point_->inode_annotation()->GetGeneration();
  LogCvmfs(kLogCvmfs, kLogDebug, "root inode is %" PRIu64,
           uint64_t(cvmfs::mount_point_->catalog_mgr()->GetRootInode()));

  cvmfs::pipe_remount_trigger_[0] = cvmfs::pipe_remount_trigger_[1] = -1;
  cvmfs::fence_remount_ = new Fence();
  auto_umount::SetMountpoint(loader_exports->mount_point);

  return loader::kFailOk;
}


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
  if (!cvmfs::mount_point_->fixed_catalog()) {
    MakePipe(cvmfs::pipe_remount_trigger_);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = cvmfs::AlarmReload;
    sa.sa_flags = SA_RESTART | SA_SIGINFO;
    sigfillset(&sa.sa_mask);
    retval = sigaction(SIGALRM, &sa, NULL);
    assert(retval == 0);
    unsigned ttl = cvmfs::mount_point_->catalog_mgr()->offline_mode() ?
      MountPoint::kShortTermTTL : cvmfs::mount_point_->GetEffectiveTtlSec();
    alarm(ttl);
    cvmfs::catalogs_valid_until_ = time(NULL) + ttl;

    cvmfs::thread_remount_trigger_ = reinterpret_cast<pthread_t *>(
      smalloc(sizeof(pthread_t)));
    retval = pthread_create(cvmfs::thread_remount_trigger_, NULL,
                            cvmfs::MainRemountTrigger, NULL);
    assert(retval == 0);
  } else {
    cvmfs::catalogs_valid_until_ = MountPoint::kIndefiniteDeadline;
  }

  cvmfs::mount_point_->download_mgr()->Spawn();
  cvmfs::mount_point_->external_download_mgr()->Spawn();
  QuotaManager *quota_mgr = cvmfs::file_system_->cache_mgr()->quota_mgr();
  quota_mgr->Spawn();
  if (quota_mgr->IsEnforcing()) {
    cvmfs::watchdog_listener_ = quota::RegisterWatchdogListener(
      quota_mgr,
      cvmfs::mount_point_->uuid()->uuid() + "-watchdog");
    cvmfs::unpin_listener_ = quota::RegisterUnpinListener(
      quota_mgr,
      cvmfs::mount_point_->catalog_mgr(),
      cvmfs::mount_point_->uuid()->uuid() + "-unpin");
  }
  cvmfs::mount_point_->tracer()->Spawn();
  cvmfs::talk_mgr_->Spawn();
  if (cvmfs::file_system_->IsNfsSource())
    nfs_maps::Spawn();
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

  delete cvmfs::talk_mgr_;
  cvmfs::talk_mgr_ = NULL;

  // The unpin listener requires the catalog, so this must be unregistered
  // before the catalog manager is removed
  if (cvmfs::unpin_listener_ != NULL) {
    quota::UnregisterListener(cvmfs::unpin_listener_);
    cvmfs::unpin_listener_ = NULL;
  }
  if (cvmfs::watchdog_listener_ != NULL) {
    quota::UnregisterListener(cvmfs::watchdog_listener_);
    cvmfs::watchdog_listener_ = NULL;
  }

  delete cvmfs::fence_remount_;
  delete cvmfs::directory_handles_;
  delete cvmfs::mount_point_;
  delete cvmfs::file_system_;
  delete cvmfs::options_mgr_;
  cvmfs::fence_remount_ = NULL;
  cvmfs::directory_handles_ = NULL;
  cvmfs::mount_point_ = NULL;
  cvmfs::file_system_ = NULL;
  cvmfs::options_mgr_ = NULL;

  if (g_monitor_ready) monitor::Fini();

  delete g_boot_error;
  g_boot_error = NULL;
  auto_umount::SetMountpoint("");
}


static int AltProcessFlavor(int argc, char **argv) {
  if (strcmp(argv[1], "__cachemgr__") == 0) {
    return PosixQuotaManager::MainCacheManager(argc, argv);
  }
  if (strcmp(argv[1], "__wpad__") == 0) {
    return download::MainResolveProxyDescription(argc, argv);
  }
  return 1;
}


static bool MaintenanceMode(const int fd_progress) {
  SendMsg2Socket(fd_progress, "Entering maintenance mode\n");
  signal(SIGALRM, SIG_IGN);
  atomic_cas32(&cvmfs::maintenance_mode_, 0, 1);
  string msg_progress =
    "Draining out kernel caches (" +
    StringifyInt(static_cast<int>(cvmfs::mount_point_->kcache_timeout_sec())) +
    "s)\n";
  SendMsg2Socket(fd_progress, msg_progress);
  SafeSleepMs(static_cast<int>(
              cvmfs::mount_point_->kcache_timeout_sec() * 1000 +
                cvmfs::kReloadSafetyMargin));
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

  if (!cvmfs::file_system_->IsNfsSource()) {
    msg_progress = "Saving inode tracker\n";
    SendMsg2Socket(fd_progress, msg_progress);
    glue::InodeTracker *saved_inode_tracker =
      new glue::InodeTracker(*cvmfs::mount_point_->inode_tracker());
    loader::SavedState *state_glue_buffer = new loader::SavedState();
    state_glue_buffer->state_id = loader::kStateGlueBufferV4;
    state_glue_buffer->state = saved_inode_tracker;
    saved_states->push_back(state_glue_buffer);
  }

  msg_progress = "Saving chunk tables\n";
  SendMsg2Socket(fd_progress, msg_progress);
  ChunkTables *saved_chunk_tables = new ChunkTables(
    *cvmfs::mount_point_->chunk_tables());
  loader::SavedState *state_chunk_tables = new loader::SavedState();
  state_chunk_tables->state_id = loader::kStateOpenFilesV4;
  state_chunk_tables->state = saved_chunk_tables;
  saved_states->push_back(state_chunk_tables);

  msg_progress = "Saving inode generation\n";
  SendMsg2Socket(fd_progress, msg_progress);
  cvmfs::inode_generation_info_.inode_generation +=
    cvmfs::mount_point_->catalog_mgr()->inode_gauge();
  cvmfs::InodeGenerationInfo *saved_inode_generation =
    new cvmfs::InodeGenerationInfo(cvmfs::inode_generation_info_);
  loader::SavedState *state_inode_generation = new loader::SavedState();
  state_inode_generation->state_id = loader::kStateInodeGeneration;
  state_inode_generation->state = saved_inode_generation;
  saved_states->push_back(state_inode_generation);

  msg_progress = "Saving open files counter\n";
  SendMsg2Socket(fd_progress, msg_progress);
  uint32_t *saved_num_fd =
    new uint32_t(cvmfs::file_system_->no_open_files()->Get());
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
      cvmfs::file_system_->no_open_dirs()->Set(
        cvmfs::directory_handles_->size());
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
        saved_inode_tracker, cvmfs::mount_point_->inode_tracker());
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV2) {
      SendMsg2Socket(fd_progress, "Migrating inode tracker (v2 to v4)... ");
      compat::inode_tracker_v2::InodeTracker *saved_inode_tracker =
        (compat::inode_tracker_v2::InodeTracker *)saved_states[i]->state;
      compat::inode_tracker_v2::Migrate(saved_inode_tracker,
                                        cvmfs::mount_point_->inode_tracker());
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV3) {
      SendMsg2Socket(fd_progress, "Migrating inode tracker (v3 to v4)... ");
      compat::inode_tracker_v3::InodeTracker *saved_inode_tracker =
        (compat::inode_tracker_v3::InodeTracker *)saved_states[i]->state;
      compat::inode_tracker_v3::Migrate(saved_inode_tracker,
                                        cvmfs::mount_point_->inode_tracker());
      SendMsg2Socket(fd_progress, " done\n");
    }

    if (saved_states[i]->state_id == loader::kStateGlueBufferV4) {
      SendMsg2Socket(fd_progress, "Restoring inode tracker... ");
      delete cvmfs::mount_point_->inode_tracker();
      glue::InodeTracker *saved_inode_tracker =
        (glue::InodeTracker *)saved_states[i]->state;
      cvmfs::mount_point_->set_inode_tracker(
        new glue::InodeTracker(*saved_inode_tracker));
      SendMsg2Socket(fd_progress, " done\n");
    }

    ChunkTables *chunk_tables = cvmfs::mount_point_->chunk_tables();

    if (saved_states[i]->state_id == loader::kStateOpenFiles) {
      SendMsg2Socket(fd_progress, "Migrating chunk tables (v1 to v4)... ");
      compat::chunk_tables::ChunkTables *saved_chunk_tables =
        (compat::chunk_tables::ChunkTables *)saved_states[i]->state;
      compat::chunk_tables::Migrate(saved_chunk_tables, chunk_tables);
      SendMsg2Socket(fd_progress,
        StringifyInt(chunk_tables->handle2fd.size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesV2) {
      SendMsg2Socket(fd_progress, "Migrating chunk tables (v2 to v4)... ");
      compat::chunk_tables_v2::ChunkTables *saved_chunk_tables =
        (compat::chunk_tables_v2::ChunkTables *)saved_states[i]->state;
      compat::chunk_tables_v2::Migrate(saved_chunk_tables, chunk_tables);
      SendMsg2Socket(fd_progress,
        StringifyInt(chunk_tables->handle2fd.size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesV3) {
      SendMsg2Socket(fd_progress, "Migrating chunk tables (v3 to v4)... ");
      compat::chunk_tables_v3::ChunkTables *saved_chunk_tables =
        (compat::chunk_tables_v3::ChunkTables *)saved_states[i]->state;
      compat::chunk_tables_v3::Migrate(saved_chunk_tables, chunk_tables);
      SendMsg2Socket(fd_progress,
        StringifyInt(chunk_tables->handle2fd.size()) + " handles\n");
    }

    if (saved_states[i]->state_id == loader::kStateOpenFilesV4) {
      SendMsg2Socket(fd_progress, "Restoring chunk tables... ");
      delete chunk_tables;
      ChunkTables *saved_chunk_tables = reinterpret_cast<ChunkTables *>(
        saved_states[i]->state);
      cvmfs::mount_point_->set_chunk_tables(
        new ChunkTables(*saved_chunk_tables));
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
      cvmfs::file_system_->no_open_files()->Set(*(reinterpret_cast<uint32_t *>(
        saved_states[i]->state)));
      SendMsg2Socket(fd_progress, " done\n");
    }
  }
  if (cvmfs::mount_point_->inode_annotation()) {
    uint64_t saved_generation = cvmfs::inode_generation_info_.inode_generation;
    cvmfs::mount_point_->inode_annotation()->IncGeneration(saved_generation);
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
        SendMsg2Socket(fd_progress, "Releasing chunk tables (version 3)\n");
        delete static_cast<compat::chunk_tables_v3::ChunkTables *>(
          saved_states[i]->state);
        break;
      case loader::kStateOpenFilesV4:
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
