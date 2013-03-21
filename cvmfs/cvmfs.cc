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
 */
// TODO: ndownload into cache

#define ENOATTR ENODATA  /**< instead of including attr/xattr.h */
#define FUSE_USE_VERSION 26
#define __STDC_FORMAT_MACROS

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
#include <inttypes.h>

#include <openssl/crypto.h>
#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>
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

#include "cvmfs.h"

#include "platform.h"
#include "logging.h"
#include "tracer.h"
#include "download.h"
#include "cache.h"
#include "nfs_maps.h"
#include "hash.h"
#include "talk.h"
#include "monitor.h"
#include "signature.h"
#include "quota.h"
#include "util.h"
#include "util_concurrency.h"
#include "atomic.h"
#include "lru.h"
#include "peers.h"
#include "dirent.h"
#include "file_chunk.h"
#include "compression.h"
#include "duplex_sqlite3.h"
#include "shortstring.h"
#include "smalloc.h"
#include "globals.h"
#include "options.h"
#include "loader.h"
#include "glue_buffer.h"

#ifdef FUSE_CAP_EXPORT_SUPPORT
#define CVMFS_NFS_SUPPORT
#else
#warning "No NFS support, Fuse too old"
#endif

using namespace std;  // NOLINT

namespace cvmfs {

const char *kDefaultCachedir = "/var/lib/cvmfs/default";
const unsigned kDefaultTimeout = 2;
const double kDefaultKCacheTimeout = 60.0;
const unsigned kDefaultNumConnections = 16;
const uint64_t kDefaultMemcache = 16*1024*1024;  // 16M RAM for meta-data caches
const uint64_t kDefaultCacheSizeMb = 1024*1024*1024;  // 1G
const unsigned kDefaultGlueBufferSize = 8192;
const unsigned int kShortTermTTL = 180;  /**< If catalog reload fails, try again
                                              in 3 minutes */
const time_t kIndefiniteDeadline = time_t(-1);

const int kMaxInitIoDelay = 32; /**< Maximum start value for exponential
                                     backoff */
const int kMaxIoDelay = 2000; /**< Maximum 2 seconds */
const int kForgetDos = 10000; /**< Clear DoS memory after 10 seconds */

const uint64_t kChunkedFileHandle = static_cast<uint64_t>(-2);
/**
 * Prevent DoS attacks on the Squid server
 */
static struct {
  time_t timestamp;
  int delay;
} previous_io_error_;
  
  
/**
 * Stores the initial catalog revision (in order to detect overflows) and
 * the incarnation (number of reloads) of the Fuse module
 */
struct InodeGenerationInfo {
  InodeGenerationInfo() {
    version = 1;
    initial_revision = 0;
    incarnation = 0;
    overflow_counter = 0;
  }
  unsigned version;
  uint64_t initial_revision;
  uint32_t incarnation;
  uint32_t overflow_counter;
};
InodeGenerationInfo inode_generation_info_;

/**
 * For cvmfs_opendir / cvmfs_readdir
 */
struct DirectoryListing {
  char *buffer;  /**< Filled by fuse_add_direntry */
  size_t size;
  size_t capacity;

  DirectoryListing() : buffer(NULL), size(0), capacity(0) { }
};

/**
 * For opening and reading chunked files
 */
class LiveFileChunk : public FileChunk {
 public:
  explicit LiveFileChunk(const FileChunk &chunk) :
    FileChunk(chunk),
    open_(false),
    file_descriptor_(0) { }

  bool Fetch() {
    file_descriptor_ = cache::FetchChunk(*this, "file chunk TODO: of what");
    if (file_descriptor_ >= 0)
      open_ = true;
    return IsOpen();
  }
  bool Close() {
    if (!IsOpen()) return true;
    open_ = close(file_descriptor_) != 0;
    return !open_;
  }
  inline int file_descriptor() const { return file_descriptor_; }

  inline bool IsOpen() const { return open_; }

  /**
   * exclusively used to find file chunks using std::lower_bound
   */
  inline bool operator<(const off_t offset) const {
    return (off_t)(this->offset() + this->size()) <= offset;
  }

 private:
  bool open_;
  int  file_descriptor_;
};
typedef std::vector<LiveFileChunk> LiveFileChunks;

const loader::LoaderExports *loader_exports_ = NULL;
bool foreground_ = false;
bool nfs_maps_ = false;
string *mountpoint_ = NULL;
string *cachedir_ = NULL;
string *tracefile_ = NULL;
string *repository_name_ = NULL;  /**< Expected repository name,
                                       e.g. atlas.cern.ch */
pid_t pid_ = 0;  /**< will be set after deamon() */
time_t boot_time_;
unsigned max_ttl_ = 0;
pthread_mutex_t lock_max_ttl_ = PTHREAD_MUTEX_INITIALIZER;
catalog::InodeGenerationAnnotation *inode_annotation_ = NULL;
cache::CatalogManager *catalog_manager_ = NULL;
lru::InodeCache *inode_cache_ = NULL;
lru::PathCache *path_cache_ = NULL;
lru::Md5PathCache *md5path_cache_ = NULL;
uint32_t glue_buffer_size_ = kDefaultGlueBufferSize;
GlueBuffer *glue_buffer_ = NULL;
CwdBuffer *cwd_buffer_ = NULL;
CwdRemountListener *cwd_remount_listener_ = NULL;
double kcache_timeout_ = kDefaultKCacheTimeout;
bool fixed_catalog_ = false;

/**
 * in maintenance mode, cache timeout is 0 and catalogs are not reloaded
 */
atomic_int32 maintenance_mode_;
atomic_int32 catalogs_expired_;
atomic_int32 drainout_mode_;
atomic_int32 reload_critical_section_;
time_t drainout_deadline_;
time_t catalogs_valid_until_;

template <typename hashed_type>
struct hash_handle {
  size_t operator() (const hashed_type handle) const {
#ifdef __x86_64__
    return MurmurHash64A(&handle, sizeof(handle), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&handle, sizeof(handle), 0x07387a4f);
#endif
  }
};
typedef google::dense_hash_map<uint64_t, DirectoryListing,
                               hash_handle<uint64_t> >
        DirectoryHandles;
DirectoryHandles *directory_handles_ = NULL;
pthread_mutex_t lock_directory_handles_ = PTHREAD_MUTEX_INITIALIZER;
uint64_t next_directory_handle_ = 0;

typedef google::dense_hash_map<fuse_ino_t, LiveFileChunks,
                               hash_handle<fuse_ino_t> >
        LiveFileChunksMap;
LiveFileChunksMap *live_file_chunks_ = NULL;
pthread_rwlock_t live_file_chunks_mutex_ = PTHREAD_RWLOCK_INITIALIZER;

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
  pthread_mutex_lock(&lock_max_ttl_);
  const unsigned current_max = max_ttl_/60;
  pthread_mutex_unlock(&lock_max_ttl_);

  return current_max;
}


void SetMaxTTL(const unsigned value) {
  pthread_mutex_lock(&lock_max_ttl_);
  max_ttl_ = value*60;
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


unsigned GetRevision() {
  return catalog_manager_->GetRevision();
};


std::string GetOpenCatalogs() {
  return catalog_manager_->PrintHierarchy();
}


void ResetErrorCounters() {
  atomic_init32(&num_io_error_);
}


static bool UseWatchdog() {
  if (loader_exports_ == NULL || loader_exports_->version < 2) {
    return true; // spawn watchdog by default
                 // Note: with library versions before 2.1.8 it might not create
                 //       stack traces properly in all cases
  }

  return ! loader_exports_->disable_watchdog;
}


void GetLruStatistics(lru::Statistics *inode_stats, lru::Statistics *path_stats,
                      lru::Statistics *md5path_stats)
{
  *inode_stats = inode_cache_->statistics();
  *path_stats = path_cache_->statistics();
  *md5path_stats = md5path_cache_->statistics();
}
  

string PrintGlueBufferStatistics() {
  return "entries: " + StringifyInt(glue_buffer_->GetNumEntries()) + "  " +
    "allocated: " + StringifyInt(glue_buffer_->GetNumBytes() / 1024) + "kB  " +
    "inserts: " + StringifyInt(glue_buffer_->GetNumInserts()) + "  " +
    glue_buffer_->GetStatistics().Print() + "\n";
}

  
string PrintCwdBufferStatistics() {
  return cwd_buffer_->GetStatistics().Print() + "\n";
}

  
std::string PrintInodeGeneration() {
  return "init-catalog-revision: " + 
    StringifyInt(inode_generation_info_.initial_revision) + "  " +
    "incarnation: " + StringifyInt(inode_generation_info_.incarnation) + "  " +
    "overflow-counter: " + StringifyInt(inode_generation_info_.overflow_counter) 
    + "\n";
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


static void AlarmReload(int signal __attribute__((unused)),
                        siginfo_t *siginfo __attribute__((unused)),
                        void *context __attribute__((unused)))
{
  atomic_cas32(&catalogs_expired_, 0, 1);
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
    drainout_deadline_ = time(NULL) + int(kcache_timeout_);
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
    inode_cache_->Pause();
    inode_cache_->Drop();
    path_cache_->Pause();
    path_cache_->Drop();
    md5path_cache_->Pause();
    md5path_cache_->Drop();
    catalog::LoadError retval = catalog_manager_->Remount(false);
    inode_annotation_->CheckForOverflow(
      catalog_manager_->GetRevision() + inode_generation_info_.incarnation, 
      inode_generation_info_.initial_revision, 
      &inode_generation_info_.overflow_counter);
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
      LogCvmfs(kLogCvmfs, kLogDebug, "reload failed, applying short term TTL");
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
  

static inline void AddToGlueBuffer(const catalog::DirectoryEntry &dirent) {
  if (atomic_read32(&drainout_mode_) || atomic_read32(&maintenance_mode_))
    glue_buffer_->AddDirent(dirent);
}
  
  
static bool GetDirentForInode(const fuse_ino_t ino,
                              catalog::DirectoryEntry *dirent,
                              bool *volatile_inode)
{
  if (volatile_inode)
    *volatile_inode = false;
  // Lookup inode in cache
  if (inode_cache_->Lookup(ino, dirent)) {
    AddToGlueBuffer(*dirent);
    return true;
  }

  // Lookup inode in catalog
  if (nfs_maps_) {
    // NFS mode
    PathString path;
    if (nfs_maps::GetPath(ino, &path) &&
        catalog_manager_->LookupPath(path, catalog::kLookupFull, dirent))
    {
      // Fix inodes
      dirent->set_inode(ino);
      catalog::DirectoryEntry parent_dirent;
      const PathString parent_path = GetParentPath(path);
      if (md5path_cache_->Lookup(hash::Md5(parent_path.GetChars(),
                                           parent_path.GetLength()),
                                 &parent_dirent))
      {
        dirent->set_parent_inode(parent_dirent.inode());
      } else {
        dirent->set_parent_inode(nfs_maps::GetInode(parent_path));
      }

      inode_cache_->Insert(ino, *dirent);
      return true;
    }
  } else {
    // Normal mode
    if (catalog_manager_->LookupInode(ino, catalog::kLookupFull, dirent)) {
      inode_cache_->Insert(ino, *dirent);
      AddToGlueBuffer(*dirent);
      return true;
    }
    
    // Lookup failed.  It might be in the glue buffer or in the cwd buffer.
    // Handling of ancient inodes from previous catalog revisions of after
    // reloading of the cvmfs module.
    // Inode should be translated into the new inode from the catalogs   
    if (inode_annotation_ && !inode_annotation_->ValidInode(ino)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "lookup for ancient inode %"PRIu64, ino);
      PathString recovered_path;
      bool found = cwd_buffer_->Find(ino, &recovered_path);
      if (found) {
        LogCvmfs(kLogCvmfs, kLogDebug, "found path %s in cwd buffer for "
                 "ancient inode %"PRIu64, recovered_path.c_str(), ino);
      } else {
        uint32_t generation = 
          catalog_manager_->GetRevision() + inode_generation_info_.incarnation;
        found = glue_buffer_->AncientInode2Path(ino, generation, 
                                                &recovered_path);
      }
      if (!found) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "internal error: " 
                 "glue buffer lookup failure (%"PRIu64"), reconstructed path %s",
                  ino, recovered_path.c_str());
      } else {
        // Path reconstructed, is it in the new file system snapshot?
        bool retval = 
          catalog_manager_->LookupPath(recovered_path, catalog::kLookupFull, 
                                       dirent);
        if (retval) {
          LogCvmfs (kLogCvmfs, kLogDebug, "translated inode %"PRIu64" to "
                    "inode %"PRIu64, ino, dirent->inode());
          dirent->set_inode(ino);
          if (volatile_inode)
            *volatile_inode = true;
          return true;
        }
      }
    }
  }

  // Can happen after reload of catalogs
  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForInode lookup failure");
  return false;
}


static bool GetDirentForPath(const PathString &path,
                             const fuse_ino_t parent_inode,
                             catalog::DirectoryEntry *dirent)
{
  hash::Md5 md5path(path.GetChars(), path.GetLength());
  if (md5path_cache_->Lookup(md5path, dirent)) {
    if (dirent->GetSpecial() == catalog::kDirentNegative)
      return false;
    AddToGlueBuffer(*dirent);
    return true;
  }

  // Lookup inode in catalog TODO: not twice md5 calculation
  if (catalog_manager_->LookupPath(path, catalog::kLookupSole, dirent)) {
    if (nfs_maps_) {
      // Fix inode
      dirent->set_inode(nfs_maps::GetInode(path));
    }
    dirent->set_parent_inode(parent_inode);
    AddToGlueBuffer(*dirent);
    md5path_cache_->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  md5path_cache_->InsertNegative(md5path);
  return false;
}


static bool GetPathForInode(const fuse_ino_t ino, PathString *path) {
  // Check the path cache first
  if (path_cache_->Lookup(ino, path)) {
    if ((atomic_read32(&drainout_mode_) || atomic_read32(&maintenance_mode_)) &&
        (ino == catalog_manager_->GetRootInode())) 
    {
      // Race condition is not ciritcal, no "wrong" data is written
      glue_buffer_->Add(ino, 0, catalog_manager_->GetRevision(), NameString());
    }
    return true;
  }

  if (nfs_maps_) {
    // NFS mode, just a lookup
    LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - lookup in NFS maps", ino);
    if (nfs_maps::GetPath(ino, path)) {
      path_cache_->Insert(ino, *path);
      return true;
    }
    return false;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "MISS %d - recursively building path", ino);

  // Find out the parent path recursively and rebuild the absolute path
  catalog::DirectoryEntry dirent;
  bool volatile_inode;
  if (!GetDirentForInode(ino, &dirent, &volatile_inode))
    return false;

  // Check if we reached the root node
  if (dirent.inode() == catalog_manager_->GetRootInode()) {
    path->Assign("", 0);
  } else {
    // Retrieve the parent path recursively
    PathString parent_path;
    if (!GetPathForInode(dirent.parent_inode(), &parent_path)) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, 
               "GetPathForInode, failed at %s (%"PRIu64")",
               dirent.name().c_str(), dirent.parent_inode());
      return false;
    }

    path->Assign(parent_path);
    path->Append("/", 1);
    path->Append(dirent.name().GetChars(), dirent.name().GetLength());
  }

  if (!volatile_inode)
    path_cache_->Insert(dirent.inode(), *path);
  return true;
}


/**
 * Find the inode number of a file name in a directory given by inode.
 * This or getattr is called as kind of prerequisit to every operation.
 * We do check catalog TTL here (and reload, if necessary).
 */
static void cvmfs_lookup(fuse_req_t req, fuse_ino_t parent,
                         const char *name)
{
  atomic_inc64(&num_fs_lookup_);
  RemountCheck();

  parent = catalog_manager_->MangleInode(parent);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_lookup in parent inode: %d for name: %s", parent, name);

  PathString path;
  PathString parent_path;
  catalog::DirectoryEntry dirent;
  struct fuse_entry_param result;

  memset(&result, 0, sizeof(result));
  double timeout = GetKcacheTimeout();
  result.attr_timeout = timeout;
  result.entry_timeout = timeout;

  // Special NFS lookups
  if ((strcmp(name, ".") == 0) || (strcmp(name, "..") == 0)) {
    if (GetDirentForInode(parent, &dirent, NULL)) {
      if (strcmp(name, ".") == 0) {
        goto reply_positive;
      } else {
        if (dirent.inode() == catalog_manager_->GetRootInode()) {
          dirent.set_inode(1);
          goto reply_positive;
        }
        if (GetDirentForInode(dirent.parent_inode(), &dirent, NULL))
          goto reply_positive;
        else
          goto reply_negative;
      }
    } else {
      goto reply_negative;
    }
  }

  if (!GetPathForInode(parent, &parent_path)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "no path for parent inode found");
    goto reply_negative;
  }

  path.Assign(parent_path);
  path.Append("/", 1);
  path.Append(name, strlen(name));
  tracer::Trace(tracer::kFuseLookup, path, "lookup()");
  if (!GetDirentForPath(path, parent, &dirent)) {
    goto reply_negative;
  }

 reply_positive:
  result.ino = dirent.inode();
  result.attr = dirent.GetStatStructure();
  fuse_reply_entry(req, &result);
  return;

 reply_negative:
  atomic_inc64(&num_fs_lookup_negative_);
  result.ino = 0;
  fuse_reply_entry(req, &result);
}


/**
 * Transform a cvmfs dirent into a struct stat.
 */
static void cvmfs_getattr(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
  atomic_inc64(&num_fs_stat_);
  RemountCheck();

  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_getattr (stat) for inode: %"PRIu64, ino);

  catalog::DirectoryEntry dirent;
  bool volatile_inode;
  const bool found = GetDirentForInode(ino, &dirent, &volatile_inode);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  struct stat info = dirent.GetStatStructure();

  fuse_reply_attr(req, &info, volatile_inode ? 0.0 : GetKcacheTimeout());
}


/**
 * Reads a symlink from the catalog.  Environment variables are expanded.
 */
static void cvmfs_readlink(fuse_req_t req, fuse_ino_t ino) {
  atomic_inc64(&num_fs_readlink_);

  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_readlink on inode: %d", ino);

  catalog::DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent, NULL);

  if (!found) {
    fuse_reply_err(req, ENOENT);
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
                            struct DirectoryListing *listing)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "Add to listing: %s", name);
  size_t remaining_size = listing->capacity - listing->size;
  const size_t entry_size = fuse_add_direntry(req, NULL, 0, name, stat_info, 0);

  while (entry_size > remaining_size) {
    listing->capacity = listing->capacity ? 2*listing->capacity : 512;
    listing->buffer =
      reinterpret_cast<char *>(srealloc(listing->buffer, listing->capacity));
    remaining_size = listing->capacity - listing->size;
  }
  fuse_add_direntry(req, listing->buffer + listing->size,
                    remaining_size, name, stat_info,
                    listing->size + entry_size);
  listing->size += entry_size;
}


/**
 * Open a directory for listing.
 */
static void cvmfs_opendir(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
  RemountCheck();
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %d", ino);

  PathString path;
  catalog::DirectoryEntry d;
  const bool found = GetPathForInode(ino, &path) && 
                     GetDirentForInode(ino, &d, NULL);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }
  if (!d.IsDirectory()) {
    fuse_reply_err(req, ENOTDIR);
    return;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %d, path %s", 
           ino, path.c_str());


  // Build listing
  DirectoryListing listing;

  // Add current directory link
  struct stat info;
  info = d.GetStatStructure();
  AddToDirListing(req, ".", &info, &listing);

  // Add parent directory link
  catalog::DirectoryEntry p;
  if (d.inode() != catalog_manager_->GetRootInode() &&
      GetDirentForInode(d.parent_inode(), &p, NULL))
  {
    info = p.GetStatStructure();
    AddToDirListing(req, "..", &info, &listing);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  if (!catalog_manager_->ListingStat(path, &listing_from_catalog)) {
    free(listing.buffer);
    fuse_reply_err(req, EIO);
    return;
  }
  for (catalog::StatEntryList::const_iterator i = listing_from_catalog.begin(),
       iEnd = listing_from_catalog.end(); i != iEnd; ++i)
  {
    if (nfs_maps_) {
      // Fix inodes
      PathString entry_path;
      entry_path.Assign(path);
      entry_path.Append("/", 1);
      entry_path.Append(i->name.GetChars(), i->name.GetLength());

      catalog::DirectoryEntry entry_dirent;
      if (!GetDirentForPath(entry_path, ino, &entry_dirent)) {
        LogCvmfs(kLogCvmfs, kLogDebug, "listing entry %s vanished, skipping",
                 entry_path.c_str());
        continue;
      }

      struct stat fixed_info = i->info;
      fixed_info.st_ino = entry_dirent.inode();
      AddToDirListing(req, i->name.c_str(), &fixed_info, &listing);
    } else {
      AddToDirListing(req, i->name.c_str(), &(i->info), &listing);
    }
  }

  // Save the directory listing and return a handle to the listing
  pthread_mutex_lock(&lock_directory_handles_);
  LogCvmfs(kLogCvmfs, kLogDebug, "linking directory handle %d to dir inode: %d", 
           next_directory_handle_, ino);
  (*directory_handles_)[next_directory_handle_] = listing;
  fi->fh = next_directory_handle_;
  ++next_directory_handle_;
  pthread_mutex_unlock(&lock_directory_handles_);
  atomic_inc64(&num_fs_dir_open_);
  atomic_inc32(&open_dirs_);

  fuse_reply_open(req, fi);
}


/**
 * Release a directory.
 */
static void cvmfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_releasedir on inode %d, handle %d",
           catalog_manager_->MangleInode(ino), fi->fh);

  int reply = 0;

  pthread_mutex_lock(&lock_directory_handles_);
  DirectoryHandles::iterator iter_handle =
    directory_handles_->find(fi->fh);
  if (iter_handle != directory_handles_->end()) {
    free(iter_handle->second.buffer);
    directory_handles_->erase(iter_handle);
    pthread_mutex_unlock(&lock_directory_handles_);
    atomic_dec32(&open_dirs_);
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
           "cvmfs_readdir on inode %d reading %d bytes from offset %d",
           catalog_manager_->MangleInode(ino), size, off);

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
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on inode: %d", ino);

  int fd = -1;
  catalog::DirectoryEntry dirent;
  PathString path;

  const bool found = GetDirentForInode(ino, &dirent, NULL) &&
                     GetPathForInode(ino, &path);

  if (!found) {
    if (fi->flags & O_CREAT)
      fuse_reply_err(req, EROFS);
    else
      fuse_reply_err(req, ENOENT);
    return;
  }

  if ((fi->flags & 3) != O_RDONLY) {
    fuse_reply_err(req, EROFS);
    return;
  }
#ifdef __APPLE__
  if ((fi->flags & O_SHLOCK) || (fi->flags & O_EXLOCK)) {
    fuse_reply_err(req, EOPNOTSUPP);
    return;
  }
#endif
  if (fi->flags & O_EXCL) {
    fuse_reply_err(req, EEXIST);
    return;
  }

  atomic_inc64(&num_fs_open_);  // Count actual open / fetch operations

  if (dirent.IsChunkedFile()) {
    // TODO: file descriptor pool and accounting
    LogCvmfs(kLogCvmfs, kLogDebug, "chunked file %s opened (download delayed "
                                   "to read() call)",
             path.c_str());

    // Retrieve File chunks from the catalog
    FileChunks chunks;
    if (!dirent.catalog()->ListFileChunks(path, &chunks) || chunks.empty()) {
      LogCvmfs(kLogCvmfs, kLogSyslog, "file %s is marked as 'chunked', but no "
                                      "chunks found in the catalog %s.",
               path.c_str(),
               dirent.catalog()->path().c_str());
      fuse_reply_err(req, EIO);
      return;
    }

    // Add process data to the retrieved chunks
    LiveFileChunks live_chunks;
    FileChunks::const_iterator i    = chunks.begin();
    FileChunks::const_iterator iend = chunks.end();
    for (; i != iend; ++i) {
      live_chunks.push_back(LiveFileChunk(*i));
    }

    // Save the newly opened file chunks into the respective list
    {
      WriteLockGuard guard(live_file_chunks_mutex_);
      if (live_file_chunks_->count(ino) == 0) {
        (*live_file_chunks_)[ino] = live_chunks;
      }
    }

    // Actual file chunks will be loaded on demand...
    fi->fh = kChunkedFileHandle;
    fuse_reply_open(req, fi);
    return;
  }

  fd = cache::FetchDirent(dirent, string(path.GetChars(), path.GetLength()));

  if (fd >= 0) {
    if (atomic_xadd32(&open_files_, 1) <
        (static_cast<int>(max_open_files_))-kNumReservedFd) {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
      // If file has changed with a new catalog, the kernel data cache needs
      // to be invalidated.  Special case: 0s metadata timeout includes no page
      // cache
      fi->keep_cache = kcache_timeout_ == 0.0 ? 0 : 1;
      if (dirent.cached_mtime() != dirent.mtime()) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "file might be new or changed, invalidating cache (%d %d %d)",
                 dirent.mtime(), dirent.cached_mtime(), ino);
        fi->keep_cache = 0;
        dirent.set_cached_mtime(dirent.mtime());
        inode_cache_->Insert(ino, dirent);
      }
      fi->fh = fd;
      fuse_reply_open(req, fi);
      return;
    } else {
      if (close(fd) == 0) atomic_dec32(&open_files_);
      LogCvmfs(kLogCvmfs, kLogSyslog, "open file descriptor limit exceeded");
      fuse_reply_err(req, EMFILE);
      return;
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "failed to open inode: %d, CAS key %s, error code %d",
             ino, dirent.checksum().ToString().c_str(), errno);
    if (errno == EMFILE) {
      fuse_reply_err(req, EMFILE);
      return;
    }
  }

  // Prevent Squid DoS
  time_t now = time(NULL);
  if (now - previous_io_error_.timestamp < kForgetDos) {
    SafeSleepMs(previous_io_error_.delay);
    if (previous_io_error_.delay < kMaxIoDelay)
      previous_io_error_.delay *= 2;
  } else {
    // Initial delay
    previous_io_error_.delay = (random() % (kMaxInitIoDelay-1)) + 2;
  }
  previous_io_error_.timestamp = now;

  atomic_inc32(&num_io_error_);
  fuse_reply_err(req, -fd);
}


/**
 * Redirected to pread into cache.
 */
static void cvmfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_read on inode: %d reading %d bytes from offset %d fd %d",
           catalog_manager_->MangleInode(ino), size, off, fi->fh);
  atomic_inc64(&num_fs_read_);

  // Get data chunk (<=4k guaranteed by Fuse)
  char *data = static_cast<char *>(alloca(size));

  unsigned int overall_bytes_fetched = 0;

  // Do we have a a chunked file?
  if (fi->fh == kChunkedFileHandle) {
    // TODO: file descriptor pool and accounting
    // TODO: read-ahead thread for chunks
    WriteLockGuard guard(live_file_chunks_mutex_);

    // find the file chunk descriptions for the requested inode
    LiveFileChunksMap::iterator chunks_itr = live_file_chunks_->find(ino);
    if (chunks_itr == live_file_chunks_->end()) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "failed to find file chunk "
                                                  "data for ino: %d",
               ino);
      fuse_reply_err(req, EINVAL);
      return;
    }
    LiveFileChunks &chunks = chunks_itr->second;

    // TODO: unlock here?
    // find the chunk that holds the beginning of the requested data
    LiveFileChunks::iterator chunk_itr = chunks.begin();
    chunk_itr = std::lower_bound(chunk_itr,
                                 chunks.end(),
                                 off);
    if (chunk_itr == chunks.end()) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "failed to find specific "
                                                  "file chunk for "
                                                  "ino: %d starting at offset: "
                                                  "%d",
               ino, off);
      fuse_reply_err(req, EIO);
      return;
    }

    // fetch all needed chunks and read the requested data
    off_t offset_in_chunk = off - chunk_itr->offset();
    do {
      assert (chunk_itr != chunks.end());
      LiveFileChunk &chunk = *chunk_itr;

      // download and open chunk on demand
      if (!chunk.IsOpen() && !chunk.Fetch()) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "failed to load chunk for "
                                                    "inode: %d, CAS key: %s, "
                                                    "error code: %d",
           ino, chunk.content_hash().ToString().c_str(), errno);

        if (errno == EMFILE) {
          fuse_reply_err(req, EMFILE);
          return;
        }

        fuse_reply_err(req, EIO);
        return;
      }

      // read data from the chunk
      assert (chunk.IsOpen());
      const size_t bytes_to_read            = size - overall_bytes_fetched;
      const size_t remaining_bytes_in_chunk = chunk.size() - offset_in_chunk;
      size_t bytes_to_read_in_chunk =
        std::min(bytes_to_read, remaining_bytes_in_chunk);
      const size_t bytes_fetched = pread(chunk.file_descriptor(),
                                         data + overall_bytes_fetched,
                                         bytes_to_read_in_chunk,
                                         offset_in_chunk);

      if (bytes_fetched == (size_t)-1) {
        LogCvmfs(kLogCvmfs, kLogDebug, "read err no %d result %d",
                 errno, bytes_fetched);
        fuse_reply_err(req, errno);
        return;
      }
      overall_bytes_fetched += bytes_fetched;

      // advance to the next chunk to keep on reading data
      chunk.Close();
      ++chunk_itr;
      offset_in_chunk = 0;
    } while (overall_bytes_fetched < size && chunk_itr != chunks.end());

  } else {
    const int64_t fd = fi->fh;
    overall_bytes_fetched = pread(fd, data, size, off);
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
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_release on inode: %d",
           catalog_manager_->MangleInode(ino));

  const int64_t fd = fi->fh;

  // do we haveh a chunked file?
  if (static_cast<uint64_t>(fd) == kChunkedFileHandle) {
    WriteLockGuard guard(live_file_chunks_mutex_);
    if (live_file_chunks_->erase(ino) > 0) {
      atomic_dec32(&open_files_);
    }
  } else {
    if (close(fd) == 0) {
      atomic_dec32(&open_files_);
    }
  }
  fuse_reply_err(req, 0);
}


static void cvmfs_statfs(fuse_req_t req, fuse_ino_t ino) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_statfs on inode: %d", ino);

  // If we return 0 it will cause the fs to be ignored in "df"
  struct statvfs info;
  memset(&info, 0, sizeof(info));

  // Unmanaged cache
  if (quota::GetCapacity() == 0) {
    fuse_reply_statfs(req, &info);
    return;
  }

  uint64_t available = 0;
  uint64_t size = quota::GetSize();
  info.f_bsize = 1;

  if (quota::GetCapacity() == (uint64_t)(-1)) {
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
    info.f_blocks = quota::GetCapacity();
    available = quota::GetCapacity() - size;
  }

  info.f_bfree = info.f_bavail = available;

  // Inodes / entries
  info.f_files = catalog_manager_->all_inodes();
  info.f_ffree = info.f_favail =
    catalog_manager_->all_inodes() - catalog_manager_->loaded_inodes();

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
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_getxattr on inode: %d for xattr: %s", ino, name);

  const string attr = name;
  catalog::DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d, NULL);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  string attribute_value;

  if (attr == "user.pid") {
    attribute_value = StringifyInt(pid_);
  } else if (attr == "user.version") {
    attribute_value = string(VERSION) + "." + string(CVMFS_PATCH_LEVEL);
  } else if (attr == "user.hash") {
    if (!d.checksum().IsNull()) {
      attribute_value = d.checksum().ToString() + " (SHA-1)";
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.lhash") {
    if (!d.checksum().IsNull()) {
      string result;
      int fd = cache::Open(d.checksum());
      if (fd < 0) {
        attribute_value = "Not in cache";
      } else {
        hash::Any hash(hash::kSha1);
        FILE *f = fdopen(fd, "r");
        if (!f) {
          fuse_reply_err(req, EIO);
          return;
        }
        if (!zlib::CompressFile2Null(f, &hash)) {
          fclose(f);
          fuse_reply_err(req, EIO);
          return;
        }
        fclose(f);
        attribute_value = hash.ToString() + " (SHA-1)";
      }
    } else {
      fuse_reply_err(req, ENOATTR);
      return;
    }
  } else if (attr == "user.revision") {
    const uint64_t revision = catalog_manager_->GetRevision();
    attribute_value = StringifyInt(revision);
  } else if (attr == "user.root_hash") {
    attribute_value = catalog_manager_->GetRootHash().ToString();
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
    attribute_value = StringifyInt(atomic_read32(&open_files_));
  } else if (attr == "user.useddirp") {
    attribute_value = StringifyInt(atomic_read32(&open_dirs_));
  } else if (attr == "user.nioerr") {
    attribute_value = StringifyInt(atomic_read32(&num_io_error_));
  } else if (attr == "user.proxy") {
    vector< vector<string> > proxy_chain;
    unsigned current_group;
    download::GetProxyInfo(&proxy_chain, &current_group);
    if (proxy_chain.size()) {
      attribute_value = proxy_chain[current_group][0];
    } else {
      attribute_value = "DIRECT";
    }
  } else if (attr == "user.host") {
    vector<string> host_chain;
    vector<int> rtt;
    unsigned current_host;
    download::GetHostInfo(&host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      attribute_value = string(host_chain[current_host]);
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
    attribute_value = StringifyInt(atomic_read64(&num_fs_open_));
  } else if (attr == "user.ndiropen") {
    attribute_value = StringifyInt(atomic_read64(&num_fs_dir_open_));
  } else if (attr == "user.ndownload") {
    attribute_value = StringifyInt(cache::GetNumDownloads());
  } else if (attr == "user.timeout") {
    unsigned seconds, seconds_direct;
    download::GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds);
  } else if (attr == "user.timeout_direct") {
    unsigned seconds, seconds_direct;
    download::GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.rx") {
    int64_t rx = uint64_t(download::GetStatistics().transferred_bytes);
    attribute_value = StringifyInt(rx/1024);
  } else if (attr == "user.speed") {
    int64_t rx = uint64_t(download::GetStatistics().transferred_bytes);
    int64_t time = uint64_t(download::GetStatistics().transfer_time);
    if (time == 0)
      attribute_value = "n/a";
    else
      attribute_value = StringifyInt((rx/1024)/time);
  } else if (attr == "user.fqrn") {
    attribute_value = *repository_name_;
  } else {
    fuse_reply_err(req, ENOATTR);
    return;
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
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_listxattr on inode: %d, size %u",
           ino, size);

  catalog::DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d, NULL);
  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  const char base_list[] = "user.pid\0user.version\0user.revision\0"
    "user.root_hash\0user.expires\0user.maxfd\0user.usedfd\0user.nioerr\0"
    "user.host\0user.proxy\0user.uptime\0user.nclg\0user.nopen\0user.ndownload\0"
    "user.timeout\0user.timeout_direct\0user.rx\0user.speed\0user.fqrn\0"
    "user.ndiropen\0";
  string attribute_list(base_list, sizeof(base_list));
  if (!d.checksum().IsNull()) {
    const char regular_file_list[] = "user.hash\0user.lhash\0";
    attribute_list += string(regular_file_list, sizeof(regular_file_list));
  }

  if (size == 0) {
    fuse_reply_xattr(req, attribute_list.length());
  } else if (size >= attribute_list.length()) {
    fuse_reply_buf(req, &attribute_list[0], attribute_list.length());
  } else {
    fuse_reply_err(req, ERANGE);
  }
}
  
  
static void cvmfs_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup) 
{
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_forget on inode: %"PRIu64", nlookup %u",
           ino, nlookup);
  // TODO: remove from cwd buffer if applicable
  fuse_reply_none(req);
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

}  // namespace cvmfs


bool g_options_ready = false;
bool g_download_ready = false;
bool g_cache_ready = false;
bool g_nfs_maps_ready = false;
bool g_peers_ready = false;
bool g_monitor_ready = false;
bool g_signature_ready = false;
bool g_quota_ready = false;
bool g_talk_ready = false;
bool g_running_created = false;

int g_fd_lockfile = -1;
void *g_sqlite_scratch = NULL;
void *g_sqlite_page_cache = NULL;
string *g_boot_error = NULL;

__attribute__ ((visibility ("default")))
loader::CvmfsExports *g_cvmfs_exports = NULL;


static int Init(const loader::LoaderExports *loader_exports) {
  int retval;
  g_boot_error = new string("unknown error");
  cvmfs::loader_exports_ = loader_exports;

  uint64_t mem_cache_size = cvmfs::kDefaultMemcache;
  unsigned timeout = cvmfs::kDefaultTimeout;
  unsigned timeout_direct = cvmfs::kDefaultTimeout;
  unsigned proxy_reset_after = 0;
  unsigned max_retries = 1;
  unsigned backoff_init = 2000;
  unsigned backoff_max = 10000;
  string tracefile = "";
  string cachedir = string(cvmfs::kDefaultCachedir);
  unsigned max_ttl = 0;
  int kcache_timeout = 0;
  bool diskless = false;
  bool rebuild_cachedb = false;
  bool nfs_source = false;
  bool shared_cache = false;
  int64_t quota_limit = cvmfs::kDefaultCacheSizeMb;
  string hostname = "localhost";
  string proxies = "";
  string public_keys = "";
  bool ignore_signature = false;
  string root_hash = "";
  bool inodes_64bit = false;

  cvmfs::boot_time_ = loader_exports->boot_time;

  // Option parsing
  options::Init();
  if (loader_exports->config_files != "") {
    vector<string> tokens = SplitString(loader_exports->config_files, ':');
    for (unsigned i = 0, s = tokens.size(); i < s; ++i) {
      options::ParsePath(tokens[i]);
    }
  } else {
    options::ParseDefault(loader_exports->repository_name);
  }
  g_options_ready = true;
  string parameter;

  // Logging
  if (options::GetValue("CVMFS_SYSLOG_LEVEL", &parameter))
    SetLogSyslogLevel(String2Uint64(parameter));
  else
    SetLogSyslogLevel(3);
  if (options::GetValue("CVMFS_SYSLOG_FACILITY", &parameter))
    SetLogSyslogFacility(String2Int64(parameter));
  if (options::GetValue("CVMFS_DEBUGLOG", &parameter))
    SetLogDebugFile(parameter);
  SetLogSyslogPrefix(loader_exports->repository_name);

  LogCvmfs(kLogCvmfs, kLogDebug, "Options:\n%s", options::Dump().c_str());

  // Overwrite default options
  if (options::GetValue("CVMFS_64BIT_INODES", &parameter))
    inodes_64bit = options::IsOn(parameter);
  if (options::GetValue("CVMFS_GLUEBUFFER_SIZE", &parameter))
    cvmfs::glue_buffer_size_ = String2Uint64(parameter);
  if (options::GetValue("CVMFS_MEMCACHE_SIZE", &parameter))
    mem_cache_size = String2Uint64(parameter) * 1024*1024;
  if (options::GetValue("CVMFS_TIMEOUT", &parameter))
    timeout = String2Uint64(parameter);
  if (options::GetValue("CVMFS_TIMEOUT_DIRECT", &parameter))
    timeout_direct = String2Uint64(parameter);
  if (options::GetValue("CVMFS_PROXY_RESET_AFTER", &parameter))
    proxy_reset_after = String2Uint64(parameter);
  if (options::GetValue("CVMFS_MAX_RETRIES", &parameter))
    max_retries = String2Uint64(parameter);
  if (options::GetValue("CVMFS_BACKOFF_INIT", &parameter))
    backoff_init = String2Uint64(parameter)*1000;
  if (options::GetValue("CVMFS_BACKOFF_MAX", &parameter))
    backoff_max = String2Uint64(parameter)*1000;
  if (options::GetValue("CVMFS_TRACEFILE", &parameter))
    tracefile = parameter;
  if (options::GetValue("CVMFS_MAX_TTL", &parameter))
    max_ttl = String2Uint64(parameter);
  if (options::GetValue("CVMFS_KCACHE_TIMEOUT", &parameter))
    kcache_timeout = String2Int64(parameter);
  if (options::GetValue("CVMFS_QUOTA_LIMIT", &parameter))
    quota_limit = String2Int64(parameter) * 1024*1024;
  if (options::GetValue("CVMFS_HTTP_PROXY", &parameter))
    proxies = parameter;
  if (options::GetValue("CVMFS_KEYS_DIR", &parameter)) {
    // Collect .pub files from CVMFS_KEYS_DIR
    public_keys = JoinStrings(FindFiles(parameter, ".pub"), ":");
  } else if (options::GetValue("CVMFS_PUBLIC_KEY", &parameter)) {
    public_keys = parameter;
  } else {
    public_keys = JoinStrings(FindFiles("/etc/cvmfs/keys", ".pub"), ":");
  }
  if (options::GetValue("CVMFS_ROOT_HASH", &parameter))
    root_hash = parameter;
  if (options::GetValue("CVMFS_DISKLESS", &parameter) &&
      options::IsOn(parameter))
  {
    diskless = true;
  }
  if (options::GetValue("CVMFS_NFS_SOURCE", &parameter) &&
      options::IsOn(parameter))
  {
    nfs_source = true;
  }
  if (options::GetValue("CVMFS_IGNORE_SIGNATURE", &parameter) &&
      options::IsOn(parameter))
  {
    ignore_signature = true;
  }
  if (options::GetValue("CVMFS_AUTO_UPDATE", &parameter) &&
      !options::IsOn(parameter))
  {
    cvmfs::fixed_catalog_ = true;
  }
  if (options::GetValue("CVMFS_SERVER_URL", &parameter)) {
    vector<string> tokens = SplitString(loader_exports->repository_name, '.');
    const string org = tokens[0];
    hostname = parameter;
    hostname = ReplaceAll(hostname, "@org@", org);
    hostname = ReplaceAll(hostname, "@fqrn@", loader_exports->repository_name);
  }
  if (options::GetValue("CVMFS_CACHE_BASE", &parameter)) {
    cachedir = MakeCanonicalPath(parameter);
    if (options::GetValue("CVMFS_SHARED_CACHE", &parameter) &&
        options::IsOn(parameter))
    {
      shared_cache = true;
      cachedir = cachedir + "/shared";
    } else {
      shared_cache = false;
      cachedir = cachedir + "/" + loader_exports->repository_name;
    }
  }

  // Fill cvmfs option variables from configuration
  cvmfs::foreground_ = loader_exports->foreground;
  cvmfs::cachedir_ = new string(cachedir);
  cvmfs::tracefile_ = new string(tracefile);
  cvmfs::repository_name_ = new string(loader_exports->repository_name);
  cvmfs::mountpoint_ = new string(loader_exports->mount_point);
  g_uid = geteuid();
  g_gid = getegid();
  cvmfs::max_ttl_ = max_ttl;
  if (kcache_timeout) {
    cvmfs::kcache_timeout_ =
      (kcache_timeout == -1) ? 0.0 : double(kcache_timeout);
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "kernel caches expire after %d seconds",
           int(cvmfs::kcache_timeout_));

  // Tune SQlite3 memory
  sqlite3_shutdown();  // Make sure SQlite starts clean after initialization
  g_sqlite_scratch = smalloc(8192*16);  // 8 KB for 8 threads (2 slots per thread)
  g_sqlite_page_cache = smalloc(1280*3275);  // 4MB
  retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, g_sqlite_scratch, 8192, 16);
  assert(retval == SQLITE_OK);
  retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, g_sqlite_page_cache,
                          1280, 3275);
  assert(retval == SQLITE_OK);
  // 4 KB
  retval = sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 32, 128);
  assert(retval == SQLITE_OK);

  // Meta-data memory caches
  const double memcache_unit_size =
    7.0 * lru::Md5PathCache::GetEntrySize() +
    lru::InodeCache::GetEntrySize() + lru::PathCache::GetEntrySize();
  const unsigned memcache_num_units =
    mem_cache_size / static_cast<unsigned>(memcache_unit_size);
  // Number of cache entries must be a multiple of 64
  const unsigned mask_64 = ~((1 << 6) - 1);
  cvmfs::inode_cache_ = new lru::InodeCache(memcache_num_units & mask_64);
  cvmfs::path_cache_ = new lru::PathCache(memcache_num_units & mask_64);
  cvmfs::md5path_cache_ =
    new lru::Md5PathCache((memcache_num_units*7) & mask_64);
  cvmfs::glue_buffer_ = new GlueBuffer(cvmfs::glue_buffer_size_);
  cvmfs::cwd_buffer_ = new CwdBuffer(*cvmfs::mountpoint_);
  cvmfs::glue_buffer_->SetCwdBuffer(cvmfs::cwd_buffer_);
  cvmfs::cwd_remount_listener_ = new CwdRemountListener(cvmfs::cwd_buffer_);

  // TODO: in loader
  cvmfs::directory_handles_ = new cvmfs::DirectoryHandles();
  cvmfs::directory_handles_->set_empty_key((uint64_t)(-1));
  cvmfs::directory_handles_->set_deleted_key((uint64_t)(-2));
  cvmfs::live_file_chunks_  = new cvmfs::LiveFileChunksMap();
  cvmfs::live_file_chunks_->set_empty_key((fuse_ino_t)(-1));
  cvmfs::live_file_chunks_->set_deleted_key((fuse_ino_t)(-2));

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

  // Create cache directory, if necessary
  if (!MkdirDeep(*cvmfs::cachedir_, 0700)) {
    *g_boot_error = "cannot create cache directory " + *cvmfs::cachedir_;
    return loader::kFailCacheDir;
  }

  // Spawn / connect to peer server
  if (diskless) {
    if (!peers::Init(GetParentPath(*cvmfs::cachedir_),
                     loader_exports->program_name, ""))
    {
      *g_boot_error = "failed to initialize peer socket";
      return loader::kFailPeers;
    }
  }
  g_peers_ready = true;

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
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                 "repository already mounted on %s",
                 cvmfs::mountpoint_->c_str());
        return loader::kFailDoubleMount;
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
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
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "looks like cvmfs has been "
             "crashed previously, rebuilding cache database");
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

  // Creates a set of cache directories (256 directories named 00..ff)
  if (!cache::Init(".")) {
    *g_boot_error = "Failed to setup cache in " + *cvmfs::cachedir_ +
                    ": " + strerror(errno);
    return loader::kFailCacheDir;
  }
  CreateFile("./.cvmfscache", 0600);
  g_cache_ready = true;

  // Start NFS maps module, if necessary
#ifdef CVMFS_NFS_SUPPORT
  if (nfs_source) {
    if (FileExists("./no_nfs_maps." + (*cvmfs::repository_name_))) {
      *g_boot_error = "Cache was used without NFS maps before. "
                      "It has to be wiped out.";
      return loader::kFailNfsMaps;
    }

    cvmfs::nfs_maps_ = true;
    const string leveldb_cache_dir = "./nfs_maps." + (*cvmfs::repository_name_);
    if (!MkdirDeep(leveldb_cache_dir, 0700)) {
      *g_boot_error = "Failed to initialize NFS maps";
      return loader::kFailNfsMaps;
    }
    if (!nfs_maps::Init(leveldb_cache_dir,
                        catalog::AbstractCatalogManager::kInodeOffset+1,
                        rebuild_cachedb))
    {
      *g_boot_error = "Failed to initialize NFS maps";
      return loader::kFailNfsMaps;
    }
    g_nfs_maps_ready = true;
  } else {
    CreateFile("./no_nfs_maps." + (*cvmfs::repository_name_), 0600);
  }
#endif

  // Init quota / managed cache
  if (quota_limit < 0)
    quota_limit = 0;
  int64_t quota_threshold = quota_limit/2;
  if (shared_cache) {
    if (!quota::InitShared(loader_exports->program_name, ".",
                           (uint64_t)quota_limit, (uint64_t)quota_threshold))
    {
      *g_boot_error = "Failed to initialize shared lru cache";
      return loader::kFailQuota;
    }
  } else {
    if (!quota::Init(".", (uint64_t)quota_limit, (uint64_t)quota_threshold,
                     rebuild_cachedb))
    {
      *g_boot_error = "Failed to initialize lru cache";
      return loader::kFailQuota;
    }
  }
  g_quota_ready = true;

  if (quota::GetSize() > quota::GetCapacity()) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "cache is already beyond quota size "
             "(size: %"PRId64", capacity: %"PRId64"), cleaning up",
             quota::GetSize(), quota::GetCapacity());
    if (!quota::Cleanup(quota_threshold)) {
      *g_boot_error = "Failed to clean up cache";
      return loader::kFailQuota;
    }
  }
  if (quota_limit) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "CernVM-FS: quota initialized, current size %luMB",
             quota::GetSize()/(1024*1024));
  }

  // Monitor, check for maximum number of open files
  if (cvmfs::UseWatchdog()) {
    if (!monitor::Init(".", *cvmfs::repository_name_, true)) {
      *g_boot_error = "failed to initialize watchdog.";
      return loader::kFailMonitor;
    }
    g_monitor_ready = true;
  }
  cvmfs::max_open_files_ = monitor::GetMaxOpenFiles();
  atomic_init32(&cvmfs::open_files_);
  atomic_init32(&cvmfs::open_dirs_);

  // Control & command interface
  if (!talk::Init(".")) {
    *g_boot_error = "failed to initialize talk socket (" +
                    StringifyInt(errno) + ")";
    return loader::kFailTalk;
  }
  g_talk_ready = true;

  // Network initialization
  download::Init(cvmfs::kDefaultNumConnections);
  download::SetHostChain(hostname);
  download::SetProxyChain(proxies);
  download::SetTimeout(timeout, timeout_direct);
  download::SetProxyGroupResetDelay(proxy_reset_after);
  download::SetRetryParameters(max_retries, backoff_init, backoff_max);
  g_download_ready = true;

  signature::Init();
  if (!signature::LoadPublicRsaKeys(public_keys)) {
    *g_boot_error = "failed to load public key(s)";
    return loader::kFailSignature;
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "CernVM-FS: using public key(s) %s",
             public_keys.c_str());
  }
  g_signature_ready = true;
  if (FileExists("/etc/cvmfs/blacklist")) {
    if (!signature::LoadBlacklist("/etc/cvmfs/blacklist")) {
      *g_boot_error = "failed to load blacklist";
      return loader::kFailSignature;
    }
  }

  // Load initial file catalog
  LogCvmfs(kLogCvmfs, kLogDebug, "fuse inode size is %d bits",
           sizeof(fuse_ino_t) * 8);
  cvmfs::inode_annotation_ =
    new catalog::InodeGenerationAnnotation(inodes_64bit ?
                                           sizeof(fuse_ino_t)*8 : 32);
  cvmfs::catalog_manager_ =
      new cache::CatalogManager(*cvmfs::repository_name_, ignore_signature);
  if (!nfs_source) {
    cvmfs::catalog_manager_->SetInodeAnnotation(cvmfs::inode_annotation_);
  }
  cvmfs::catalog_manager_->RegisterRemountListener(cvmfs::cwd_remount_listener_);  
  if (root_hash != "") {
    cvmfs::fixed_catalog_ = true;
    hash::Any hash(hash::kSha1, hash::HexPtr(string(root_hash)));
    retval = cvmfs::catalog_manager_->InitFixed(hash);
  } else {
    retval = cvmfs::catalog_manager_->Init();
  }
  if (!retval) {
    *g_boot_error = "Failed to initialize root file catalog";
    return loader::kFailCatalog;
  }
  cvmfs::inode_generation_info_.initial_revision = 
    cvmfs::catalog_manager_->GetRevision();
  LogCvmfs(kLogCvmfs, kLogDebug, "root inode is %"PRIu64, 
           cvmfs::catalog_manager_->GetRootInode());

  return loader::kFailOk;
}


/**
 * Things that have to be executed after fork() / daemon()
 */
static void Spawn() {
  int retval;

  // Setup catalog reload alarm (_after_ fork())
  atomic_init32(&cvmfs::maintenance_mode_);
  atomic_init32(&cvmfs::drainout_mode_);
  atomic_init32(&cvmfs::reload_critical_section_);
  atomic_init32(&cvmfs::catalogs_expired_);
  if (!cvmfs::fixed_catalog_) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = cvmfs::AlarmReload;
    sa.sa_flags = SA_SIGINFO;
    sigfillset(&sa.sa_mask);
    retval = sigaction(SIGALRM, &sa, NULL);
    assert(retval == 0);
    unsigned ttl = cvmfs::catalog_manager_->offline_mode() ?
      cvmfs::kShortTermTTL : cvmfs::GetEffectiveTTL();
    alarm(ttl);
    cvmfs::catalogs_valid_until_ = time(NULL) + ttl;
  } else {
    cvmfs::catalogs_valid_until_ = cvmfs::kIndefiniteDeadline;
  }

  cvmfs::pid_ = getpid();
  if (cvmfs::UseWatchdog() && g_monitor_ready) {
    monitor::Spawn();
  }
  download::Spawn();
  quota::Spawn();
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
  signal(SIGALRM, SIG_DFL);
  tracer::Fini();
  if (g_signature_ready) signature::Fini();
  if (g_download_ready) download::Fini();
  if (g_talk_ready) talk::Fini();
  if (g_monitor_ready) monitor::Fini();
  if (g_quota_ready) quota::Fini();
  if (g_nfs_maps_ready) nfs_maps::Fini();
  if (g_cache_ready) cache::Fini();
  if (g_running_created)
    unlink(("running." + *cvmfs::repository_name_).c_str());
  if (g_fd_lockfile >= 0) UnlockFile(g_fd_lockfile);
  if (g_peers_ready) peers::Fini();
  if (g_options_ready) options::Fini();

  delete cvmfs::catalog_manager_;
  delete cvmfs::inode_annotation_;
  delete cvmfs::directory_handles_;
  delete cvmfs::live_file_chunks_;
  delete cvmfs::glue_buffer_;
  delete cvmfs::cwd_buffer_;
  delete cvmfs::cwd_remount_listener_;
  delete cvmfs::path_cache_;
  delete cvmfs::inode_cache_;
  delete cvmfs::md5path_cache_;
  cvmfs::catalog_manager_ = NULL;
  cvmfs::inode_annotation_ = NULL;
  cvmfs::directory_handles_ = NULL;
  cvmfs::live_file_chunks_ = NULL;
  cvmfs::glue_buffer_ = NULL;
  cvmfs::cwd_buffer_ = NULL;
  cvmfs::cwd_remount_listener_ = NULL;
  cvmfs::path_cache_ = NULL;
  cvmfs::inode_cache_ = NULL;
  cvmfs::md5path_cache_ = NULL;

  sqlite3_shutdown();
  if (g_sqlite_page_cache) free(g_sqlite_page_cache);
  if (g_sqlite_scratch) free(g_sqlite_scratch);
  g_sqlite_page_cache = NULL;
  g_sqlite_scratch = NULL;

  delete g_boot_error;
  g_boot_error = NULL;
}


static int AltProcessFlavor(int argc, char **argv) {
  if (strcmp(argv[1], "__peersrv__") == 0) {
    return peers::MainPeerServer(argc, argv);
  }
  if (strcmp(argv[1], "__cachemgr__") == 0) {
    return quota::MainCacheManager(argc, argv);
  }
  return 1;
}


static bool MaintenanceMode(const int fd_progress) {
  SendMsg2Socket(fd_progress, "Entering maintenance mode\n");
  signal(SIGALRM, SIG_DFL);
  atomic_cas32(&cvmfs::maintenance_mode_, 0, 1);
  string msg_progress = "Draining out kernel caches (" +
                        StringifyInt((int)cvmfs::kcache_timeout_) + "s)\n";
  SendMsg2Socket(fd_progress, msg_progress);
  SafeSleepMs((int)cvmfs::kcache_timeout_*1000);
  return true;
}


static bool SaveState(const int fd_progress, loader::StateList *saved_states) {
  string msg_progress;
  
  unsigned num_open_dirs = cvmfs::directory_handles_->size();
  if (num_open_dirs != 0) {
#ifdef DEBUGMSG
    for (cvmfs::DirectoryHandles::iterator i = cvmfs::directory_handles_->begin(),
         iEnd = cvmfs::directory_handles_->end(); i != iEnd; ++i)
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "saving dirhandle %d", i->first);
    }
#endif
    
    msg_progress = "Saving open directory handles (" +
      StringifyInt(num_open_dirs) + " handles)\n";
    SendMsg2Socket(fd_progress, msg_progress);

    // TODO: should rather be saved just in a malloc'd memory block
    cvmfs::DirectoryHandles *saved_handles =
      new cvmfs::DirectoryHandles(*cvmfs::directory_handles_);
    loader::SavedState *save_open_dirs = new loader::SavedState();
    save_open_dirs->state_id = loader::kStateOpenDirs;
    save_open_dirs->state = saved_handles;
    saved_states->push_back(save_open_dirs);
  }
  
  msg_progress = "Saving glue buffer\n";
  SendMsg2Socket(fd_progress, msg_progress);
  GlueBuffer *saved_glue_buffer = new GlueBuffer(*cvmfs::glue_buffer_);
  loader::SavedState *state_glue_buffer = new loader::SavedState();
  state_glue_buffer->state_id = loader::kStateGlueBuffer;
  state_glue_buffer->state = saved_glue_buffer;
  saved_states->push_back(state_glue_buffer);
  
  msg_progress = "Saving cwd buffer\n";
  SendMsg2Socket(fd_progress, msg_progress);
  cvmfs::cwd_buffer_->BeforeRemount(cvmfs::catalog_manager_);
  CwdBuffer *saved_cwd_buffer = new CwdBuffer(*cvmfs::cwd_buffer_);
  loader::SavedState *state_cwd_buffer = new loader::SavedState();
  state_cwd_buffer->state_id = loader::kStateCwdBuffer;
  state_cwd_buffer->state = saved_cwd_buffer;
  saved_states->push_back(state_cwd_buffer);
  
  msg_progress = "Saving inode generation\n";
  SendMsg2Socket(fd_progress, msg_progress);
  cvmfs::InodeGenerationInfo *saved_inode_generation = 
    new cvmfs::InodeGenerationInfo(cvmfs::inode_generation_info_);
  loader::SavedState *state_inode_generation = new loader::SavedState();
  state_inode_generation->state_id = loader::kStateInodeGeneration;
  state_inode_generation->state = saved_inode_generation;
  saved_states->push_back(state_inode_generation);  

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

      SendMsg2Socket(fd_progress,
        StringifyInt(cvmfs::directory_handles_->size()) + " handles\n");
    }
    
    if (saved_states[i]->state_id == loader::kStateGlueBuffer) {
      SendMsg2Socket(fd_progress, "Restoring glue buffer... ");
      delete cvmfs::glue_buffer_;
      GlueBuffer *saved_glue_buffer = (GlueBuffer *)saved_states[i]->state;
      cvmfs::glue_buffer_ = new GlueBuffer(*saved_glue_buffer);
      cvmfs::glue_buffer_->Resize(cvmfs::glue_buffer_size_);
      cvmfs::glue_buffer_->SetCwdBuffer(cvmfs::cwd_buffer_);
      SendMsg2Socket(fd_progress, " done\n");
    }
    
    if (saved_states[i]->state_id == loader::kStateCwdBuffer) {
      SendMsg2Socket(fd_progress, "Restoring cwd buffer... ");
      delete cvmfs::cwd_remount_listener_;
      delete cvmfs::cwd_buffer_;
      CwdBuffer *saved_cwd_buffer = (CwdBuffer *)saved_states[i]->state;
      cvmfs::cwd_buffer_ = new CwdBuffer(*saved_cwd_buffer);
      cvmfs::glue_buffer_->SetCwdBuffer(cvmfs::cwd_buffer_);
      cvmfs::cwd_remount_listener_ = new CwdRemountListener(cvmfs::cwd_buffer_);
      cvmfs::catalog_manager_->RegisterRemountListener(
        cvmfs::cwd_remount_listener_);
      SendMsg2Socket(fd_progress, " done\n");
    }
    
    if (saved_states[i]->state_id == loader::kStateInodeGeneration) {
      SendMsg2Socket(fd_progress, "Restoring inode generation... ");
      cvmfs::inode_generation_info_ = 
        *((cvmfs::InodeGenerationInfo *)saved_states[i]->state);
      uint32_t incarnation = ++cvmfs::inode_generation_info_.incarnation;
      cvmfs::catalog_manager_->SetIncarnation(incarnation);
      SendMsg2Socket(fd_progress, " done\n");
    }
  }
  cvmfs::inode_annotation_->CheckForOverflow(
    cvmfs::catalog_manager_->GetRevision() + 
      cvmfs::inode_generation_info_.incarnation, 
    cvmfs::inode_generation_info_.initial_revision, 
    &cvmfs::inode_generation_info_.overflow_counter);

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
        SendMsg2Socket(fd_progress, "Releasing saved glue buffer\n");
        delete static_cast<GlueBuffer *>(saved_states[i]->state);
        break;
      case loader::kStateCwdBuffer:
        SendMsg2Socket(fd_progress, "Releasing saved cwd buffer\n");
        delete static_cast<CwdBuffer *>(saved_states[i]->state);
        break;
      case loader::kStateInodeGeneration:
        SendMsg2Socket(fd_progress, "Releasing saved inode generation info\n");
        delete static_cast<cvmfs::InodeGenerationInfo *>(saved_states[i]->state);
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
