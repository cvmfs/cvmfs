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
#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

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
#include "fuse_op_stubs.h"
#include "inode_cache.h"
#include "path_cache.h"
#include "md5path_cache.h"
#include "RemoteCatalogManager.h"
#include "DirectoryEntry.h"
#include "compression.h"
#include "duplex_sqlite3.h"
#include "smalloc.h"

using namespace std;  // NOLINT

namespace cvmfs {

const unsigned int kInodeCacheSize = 20000;
const unsigned int kPathCacheSize = 1000;
const unsigned int kMd5pathCacheSize = 1000;

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

/**
 * For cvmfs_opendir / cvmfs_readdir
 */
struct DirectoryListing {
  char *buffer;  /**< Filled by fuse_add_direntry */
  size_t size;
  size_t capacity;

  DirectoryListing() : buffer(NULL), size(0), capacity(0) { }
};

string *mountpoint_ = NULL;
string *root_url_ = NULL;
string *cachedir_ = NULL;
string *tracefile_ = NULL;
string *repository_name_ = NULL;  /**< Expected repository name,
                                       e.g. atlas.cern.ch */
uid_t uid_ = 0;  /**< will be set to uid of launching user. */
gid_t gid_ = 0;  /**< will be set to gid of launching user. */
pid_t pid_ = 0;  /**< will be set after deamon() */
time_t boot_time_;
unsigned max_ttl_ = 0;
pthread_mutex_t lock_max_ttl_ = PTHREAD_MUTEX_INITIALIZER;
RemoteCatalogManager *catalog_manager_;
InodeCache *inode_cache_ = NULL;
PathCache *path_cache_ = NULL;
Md5PathCache *md5path_cache_ = NULL;

map<uint64_t, DirectoryListing> *directory_handles_ = NULL;
pthread_mutex_t lock_directory_handles_ = PTHREAD_MUTEX_INITIALIZER;
uint64_t next_directory_handle_ = 0;

atomic_int64 num_open_;
atomic_int64 num_download_;
atomic_int32 num_io_error_;
atomic_int32 open_files_; /**< number of currently open files by Fuse calls */
unsigned max_open_files_; /**< maximum allowed number of open files */
const int kNumReservedFd = 512;  /**< Number of reserved file descriptors for
                                      internal use */


unsigned GetMaxTtl() {
  pthread_mutex_lock(&lock_max_ttl_);
  const unsigned current_max = max_ttl_/60;
  pthread_mutex_unlock(&lock_max_ttl_);

  return current_max;
}


void SetMaxTtl(const unsigned value) {
  pthread_mutex_lock(&lock_max_ttl_);
  max_ttl_ = value*60;
  pthread_mutex_unlock(&lock_max_ttl_);
}


static bool GetDirentForInode(const fuse_ino_t ino, DirectoryEntry *dirent) {
  // Lookup inode in cache
  if (inode_cache_->lookup(ino, dirent)) {
    LogCvmfs(kLogInodeCache, kLogDebug, "HIT %d -> '%s'",
             ino, dirent->name().c_str());
    return true;
  } else {
    LogCvmfs(kLogInodeCache, kLogDebug, "MISS %d --> lookup in catalogs",
             ino);

    // Lookup inode in catalog
    if (catalog_manager_->Lookup(ino, dirent)) {
      LogCvmfs(kLogInodeCache, kLogDebug, "CATALOG HIT %d -> '%s'",
               dirent->inode(), dirent->name().c_str());
      inode_cache_->insert(ino, *dirent);
      return true;
    } else {
      LogCvmfs(kLogInodeCache, kLogDebug,
               "no entry --> maybe data corruption?");
      return false;
    }
  }
  return false;  // unreachable
}


static bool get_dirent_for_path(const string &path, DirectoryEntry *dirent) {
  /*
   *  this one is pretty nasty!
   *  in a unit test ../../test/unittests/02....cc
   *  the cache showed reasonable performance (1.2 millon transactions in 4 seconds)
   *  but here it SLOWS DOWN the Davinci benchmark about 20 seconds
   *
   *  there must either be something wrong in the code itself or
   *  I must have overseen some cache coherency problem.
   *
   *  I.e. the data coming out of it is somehow corrupt.
   *  But this is very unlikely,
   *  because the tests do not fail, they are just slower.
   */

  // check the md5path_cache first
  // (TODO: this is a quick and dirty prototype currently!!)
  // it actually slows down the stuff... TODO: find out why
  // if (md5path_cache_->lookup(md5, dirent)) {
  //          if (dirent.catalog_id == -1) {
  //             pmesg(D_MD5_CACHE, "HIT NEGATIVE %s -> '%s'",
  //                   md5.to_string().c_str(), dirent.name.c_str());
  //             return false;
  //
  //          } else {
  //             pmesg(D_MD5_CACHE, "HIT %s -> '%s'",
  //                   md5.to_string().c_str(), dirent.name.c_str());
  //             return true;
  //          }
  //       } else {
  LogCvmfs(kLogMd5Cache, kLogDebug, "MISS %s --> lookup in catalogs",
           path.c_str());

  if (catalog_manager_->Lookup(path, dirent)) {
    LogCvmfs(kLogMd5Cache, kLogDebug, "CATALOG HIT %s -> '%s'",
             path.c_str(), dirent->name().c_str());
    //            md5path_cache_->insert(md5, dirent);
    return true;
  } else {
    // struct catalog::t_dirent negative;
    // negative.catalog_id = -1;
    // negative.name = "negative!";
    //            md5path_cache_->insert(md5, negative);
    return false;
  }
  // }

  return false;
}

static bool GetPathForInode(const fuse_ino_t ino, string *path) {
  // Check the path cache first
  if (path_cache_->lookup(ino, path)) {
    LogCvmfs(kLogPathCache, kLogDebug, "HIT %d -> '%s'", ino, path->c_str());
    return true;
  }

  LogCvmfs(kLogPathCache, kLogDebug, "MISS %d - recursively building path",
           ino);

  // Find out the parent path recursively and rebuild the absolute path
  string parent_path;
  DirectoryEntry dirent;

  if (!GetDirentForInode(ino, &dirent)) {
    return false;
  }

  // Check if we reached the root node
  if (dirent.inode() == catalog_manager_->GetRootInode()) {
    *path = "";
  } else {
    // Retrieve the parent path recursively
    if (!GetPathForInode(dirent.parent_inode(), &parent_path)) {
      return false;
    }

    *path = parent_path + "/" + dirent.name();
  }

  path_cache_->insert(dirent.inode(), *path);
  return true;
}


/**
 * Find the inode number of a file name in a directory given by inode.
 */
static void cvmfs_lookup(fuse_req_t req, fuse_ino_t parent,
                         const char *name)
{
  parent = catalog_manager_->MangleInode(parent);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_lookup in parent inode: %d for name: %s", parent, name);

  string parent_path;

  if (!GetPathForInode(parent, &parent_path)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "no path for parent inode found");

    fuse_reply_err(req, ENOENT);
    return;
  }

  DirectoryEntry dirent;
  const string path = parent_path + "/" + name;
  // TODO: getdirent for path?
  const bool found_entry = catalog_manager_->LookupWithoutParent(path, &dirent);
  if (!found_entry) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  dirent.set_parent_inode(parent);

  inode_cache_->insert(dirent.inode(), dirent);
  path_cache_->insert(dirent.inode(), path);

  struct fuse_entry_param result;
  memset(&result, 0, sizeof(result));
  result.ino = dirent.inode();
  result.attr = dirent.GetStatStructure();
  result.attr_timeout = 2.0;  // TODO(rene): replace these magic numbers
  result.entry_timeout = 2.0;

  fuse_reply_entry(req, &result);
}


/**
 * Gets called as kind of prerequisit to every operation.
 * We do two kinds of magic here: check catalog TTL (and reload, if necessary)
 * and load nested catalogs.  Nested catalogs may also be loaded on readdir.
 *
 * Also, we insert things in our d-cache here.  It is not sufficient to do
 * all the inserts here, even though stat will be called before anything else;
 * they might be cached by the kernel.
 */
static void cvmfs_getattr(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_getattr (stat) for inode: %d", ino);

  DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  struct stat info = dirent.GetStatStructure();

  fuse_reply_attr(req, &info, 1.0);  // TODO(rene): replace magic number
}


/**
 * Reads a symlink from the catalog.  Environment variables are expanded.
 */
static void cvmfs_readlink(fuse_req_t req, fuse_ino_t ino) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_readlink on inode: %d", ino);
  tracer::Trace(tracer::kFuseReadlink, "no path provided", "readlink() call");

  DirectoryEntry dirent;
  const bool found = GetDirentForInode(ino, &dirent);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  if (!dirent.IsLink()) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  fuse_reply_readlink(req, dirent.ExpandSymlink().c_str());
}


static void AddToDirListing(const fuse_req_t req,
                            const char *name, const struct stat *stat_info,
                            struct DirectoryListing *listing)
{
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
                          struct fuse_file_info *fi) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_opendir on inode: %d", ino);

  string path;
  DirectoryEntry d;
  const bool found = GetPathForInode(ino, &path) && GetDirentForInode(ino, &d);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  if (!d.IsDirectory()) {
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  // Build listing
  DirectoryListing listing;

  // Add current directory link
  struct stat info;
  info = d.GetStatStructure();
  AddToDirListing(req, ".", &info, &listing);

  // Add parent directory link
  DirectoryEntry p;
  if (d.inode() != catalog_manager_->GetRootInode() &&
      GetDirentForInode(d.parent_inode(), &p)) {
    info = p.GetStatStructure();
    AddToDirListing(req, "..", &info, &listing);
  }

  // Add all names
  DirectoryEntryList listing_from_catalog;
  if (!catalog_manager_->Listing(path, &listing_from_catalog)) {
    free(listing.buffer);
    fuse_reply_err(req, EIO);
    return;
  }
  for (DirectoryEntryList::const_iterator i = listing_from_catalog.begin(),
       iEnd = listing_from_catalog.end(); i != iEnd; ++i)
  {
    info = i->GetStatStructure();
    AddToDirListing(req, i->name().c_str(), &info, &listing);
  }

  // Save the directory listing and return a handle to the listing
  pthread_mutex_lock(&lock_directory_handles_);
  (*directory_handles_)[next_directory_handle_] = listing;
  fi->fh = next_directory_handle_;
  ++next_directory_handle_;
  pthread_mutex_unlock(&lock_directory_handles_);

  fuse_reply_open(req, fi);
}


/**
 * Release a directory.
 */
static void cvmfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_releasedir on inode: %d",
           catalog_manager_->MangleInode(ino));

  int reply = 0;

  pthread_mutex_lock(&lock_directory_handles_);
  map<uint64_t, DirectoryListing>::iterator iter_handle =
    directory_handles_->find(fi->fh);
  if (iter_handle != directory_handles_->end()) {
    free(iter_handle->second.buffer);
    directory_handles_->erase(iter_handle);
    reply = 0;
  } else {
    reply = EINVAL;
  }
  pthread_mutex_unlock(&lock_directory_handles_);

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
    LogCvmfs(kLogCvmfs, kLogDebug, "DATA Reply");
    fuse_reply_buf(req, buffer + offset,
                   std::min(buffer_size - offset, max_size));
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "NULL Reply");
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
  map<uint64_t, DirectoryListing>::const_iterator iter_handle =
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
 * \return Read-only file descriptor in fi->fh
 */
static void cvmfs_open(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_open on inode: %d", ino);
  tracer::Trace(tracer::kFuseOpen, "no path provided", "open() call");

  int fd = -1;
  DirectoryEntry d;
  string path;

  const bool found = GetDirentForInode(ino, &d) && GetPathForInode(ino, &path);

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

  fd = cache::Fetch(d, path);
  atomic_inc64(&num_open_);

  if (fd >= 0) {
    if (atomic_xadd32(&open_files_, 1) <
        (static_cast<int>(max_open_files_))-kNumReservedFd) {
      LogCvmfs(kLogCvmfs, kLogDebug, "file %s opened (fd %d)",
               path.c_str(), fd);
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
             ino, d.checksum().ToString().c_str(), errno);
    if (errno == EMFILE) {
      fuse_reply_err(req, EMFILE);
      return;
    }
  }

  // Prevent Squid DoS
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
  fuse_reply_err(req, EIO);
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
  tracer::Trace(tracer::kFuseRead, "no path provided", "read() call");

  // Get data chunk (<=4k guaranteed by Fuse)
  char *data = static_cast<char *>(alloca(size));
  const int64_t fd = fi->fh;
  int result = pread(fd, data, size, off);

  // Push it to user
  if (result >= 0) {
    fuse_reply_buf(req, data, result);
    LogCvmfs(kLogCvmfs, kLogDebug, "pushed %d bytes to user", result);
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "read err no %d result %d", errno, result);
    fuse_reply_err(req, errno);
  }
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
  if (close(fd) == 0) atomic_dec32(&open_files_);

  fuse_reply_err(req, 0);
}


/**
 * Emulates the getattr walk done by Fuse.
 */
static int walk_path(const string &path) {
  //      struct stat info;
  // if ((path == "") || (path == "/"))
  //    return cvmfs_getattr("/", &info);

  int attr_result = walk_path(GetParentPath(path));
  // if (attr_result == 0)
  //    return cvmfs_getattr(path.c_str(), &info);

  return attr_result;
}


/**
 * Removes a file from local cache
 * TODO
 */
int ClearFile(const string &path) {
  /*  int attr_result = walk_path(path);
   if (attr_result != 0)
   return attr_result;

   const hash::t_md5 md5(catalog::mangled_path(path));
   int result;

   catalog::lock();

   catalog::t_dirent d;
   if (catalog::lookup_informed_unprotected(md5, find_catalog_id(path), d)) {
   if ((!(d.flags & catalog::FILE)) || (d.flags & catalog::FILE_LINK)) {
   result = -EINVAL;
   } else {
   quota::remove(d.checksum);
   result = 0;
   }
   } else {
   result = -ENOENT;
   }

   catalog::unlock();

   return result;*/
  return 0;
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
  DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);

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
  } else if (attr == "user.expires") {
    time_t expires = 0;
    // TODO(rene): remove that or implement it properly
    // catalog_manager_->GetExpireTime();
    time_t now = time(NULL);
    attribute_value = StringifyInt((expires-now)/60);
  } else if (attr == "user.maxfd") {
    attribute_value = StringifyInt(max_open_files_ - kNumReservedFd);
  } else if (attr == "user.usedfd") {
    attribute_value = StringifyInt(atomic_read32(&open_files_));
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
    const int num_catalogs = catalog_manager_->GetNumberOfAttachedCatalogs();
    attribute_value = StringifyInt(num_catalogs);
  } else if (attr == "user.nopen") {
    attribute_value = StringifyInt(atomic_read64(&num_open_));
  } else if (attr == "user.ndownload") {
    attribute_value = StringifyInt(atomic_read64(&num_download_));
  } else if (attr == "user.timeout") {
    unsigned seconds, seconds_direct;
    download::GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds);
  } else if (attr == "user.timeout_direct") {
    unsigned seconds, seconds_direct;
    download::GetTimeout(&seconds, &seconds_direct);
    attribute_value = StringifyInt(seconds_direct);
  } else if (attr == "user.rx") {
    int64_t rx = download::GetTransferredBytes();
    attribute_value = StringifyInt(rx/1024);
  } else if (attr == "user.speed") {
    int64_t rx = download::GetTransferredBytes();
    int64_t time = download::GetTransferTime();
    if (time == 0)
      attribute_value = "n/a";
    else
      attribute_value = StringifyInt((rx/1024)/time);
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

  DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);
  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  const char base_list[] = "user.pid\0user.version\0user.revision\0"
    "user.expires\0user.maxfd\0user.usedfd\0user.nioerr\0user.host\0"
    "user.uptime\0user.nclg\0user.nopen\0user.ndownload\0user.timeout\0"
    "user.timeout_direct\0user.rx\0user.speed\0";
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


static void cvmfs_forget(fuse_req_t req, fuse_ino_t ino,
                         unsigned long nlookup)  // NOLINT(runtime/int)
{
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_forget on inode: %d", ino);
  fuse_reply_none(req);
}


static void cvmfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
  ino = catalog_manager_->MangleInode(ino);
  LogCvmfs(kLogCvmfs, kLogDebug,
           "cvmfs_access on inode: %d asking for R: %s W: %s X: %s", ino,
           ((mask & R_OK) ? "yes" : "no"),
           ((mask & W_OK) ? "yes" : "no"),
           ((mask & X_OK) ? "yes" : "no"));

  DirectoryEntry d;
  const bool found = GetDirentForInode(ino, &d);

  if (!found) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  // Check access rights for owner (RWX access bits shifted six to the left)
  unsigned int mask_for_owner = mask << 6;
  if ((mask & W_OK) || (mask_for_owner & d.mode()) != mask_for_owner) {
    fuse_reply_err(req, EACCES);
    return;
  }

  fuse_reply_err(req, 0);
}


/**
 * Do after-daemon() initialization
 */
static void cvmfs_init(void *userdata, struct fuse_conn_info *conn) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_init");
  int retval;

  // Switch back to cache dir after daemon()
  retval = chdir(cachedir_->c_str());
  assert(retval == 0);

  pid_ = getpid();
  monitor::Spawn();
  download::Spawn();
  quota::Spawn();
  talk::Spawn();

  if (*tracefile_ != "")
    tracer::Init(8192, 7000, *tracefile_);
  else
    tracer::InitNull();
}

static void cvmfs_destroy(void *unused __attribute__((unused))) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_destroy");
  tracer::Fini();
}

/**
 * Puts the callback functions in one single structure
 */
static void set_cvmfs_ops(struct fuse_lowlevel_ops *cvmfs_operations) {
  // Init/Fini
  cvmfs_operations->init     = cvmfs_init;
  cvmfs_operations->destroy  = cvmfs_destroy;

  // Implemented
  cvmfs_operations->lookup      = cvmfs_lookup;
  cvmfs_operations->forget      = cvmfs_forget;
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
  cvmfs_operations->access      = cvmfs_access;

  // Stubs
  cvmfs_operations->setattr     = cvmfs_setattr;
  cvmfs_operations->mknod       = cvmfs_mknod;
  cvmfs_operations->mkdir       = cvmfs_mkdir;
  cvmfs_operations->unlink      = cvmfs_unlink;
  cvmfs_operations->rmdir       = cvmfs_rmdir;
  cvmfs_operations->symlink     = cvmfs_symlink;
  cvmfs_operations->rename      = cvmfs_rename;
  cvmfs_operations->link        = cvmfs_link;
  cvmfs_operations->write       = cvmfs_write;
  cvmfs_operations->flush       = cvmfs_flush;
  cvmfs_operations->fsync       = cvmfs_fsync;
  cvmfs_operations->fsyncdir    = cvmfs_fsyncdir;
  cvmfs_operations->setxattr    = cvmfs_setxattr;
  cvmfs_operations->removexattr = cvmfs_removexattr;
  cvmfs_operations->create      = cvmfs_create;
  cvmfs_operations->getlk       = cvmfs_getlk;
  cvmfs_operations->setlk       = cvmfs_setlk;
  cvmfs_operations->bmap        = cvmfs_bmap;
  cvmfs_operations->ioctl       = cvmfs_ioctl;
  cvmfs_operations->poll        = cvmfs_poll;
}

}  // namespace cvmfs


/**
 * One single structure to contain the file system options.
 * Strings(char *) must be deallocated by the user
 */
struct CvmfsOptions {
  unsigned timeout;
  unsigned timeout_direct;
  unsigned max_ttl;
  char     *hostname;
  char     *cachedir;
  char     *proxies;
  char     *tracefile;
  char     *pubkey;
  char     *logfile;
  char     *blacklist;
  char     *repo_name;
  int      force_signing;
  int      rebuild_cachedb;
  int      nofiles;
  int      grab_mountpoint;
  int      syslog_level;
  int      kernel_cache;
  int      auto_cache;
  int      entry_timeout;
  int      attr_timeout;
  int      negative_timeout;
  int      use_ino;

  int64_t  quota_limit;
  int64_t  quota_threshold;
};

// Follow the fuse convention for option parsing
enum {
  KEY_HELP,
  KEY_VERSION,
  KEY_FOREGROUND,
  KEY_SINGLETHREAD,
  KEY_DEBUG,
};
#define CVMFS_OPT(t, p, v) { t, offsetof(struct CvmfsOptions, p), v }
#define CVMFS_SWITCH(t, p) { t, offsetof(struct CvmfsOptions, p), 1 }
static struct fuse_opt cvmfs_array_opts[] = {
  CVMFS_OPT("timeout=%u",          timeout, 2),
  CVMFS_OPT("timeout_direct=%u",   timeout_direct, 2),
  CVMFS_OPT("max_ttl=%u",          max_ttl, 0),
  CVMFS_OPT("cachedir=%s",         cachedir, 0),
  CVMFS_OPT("proxies=%s",          proxies, 0),
  CVMFS_OPT("tracefile=%s",        tracefile, 0),
  CVMFS_SWITCH("force_signing",    force_signing),
  CVMFS_OPT("pubkey=%s",           pubkey, 0),
  CVMFS_OPT("logfile=%s",          logfile, 0),
  CVMFS_SWITCH("rebuild_cachedb",  rebuild_cachedb),
  CVMFS_OPT("quota_limit=%ld",     quota_limit, 0),
  CVMFS_OPT("quota_threshold=%ld", quota_threshold, 0),
  CVMFS_OPT("nofiles=%d",          nofiles, 0),
  CVMFS_SWITCH("grab_mountpoint",  grab_mountpoint),
  CVMFS_OPT("repo_name=%s",        repo_name, 0),
  CVMFS_OPT("blacklist=%s",        blacklist, 0),
  CVMFS_OPT("syslog_level=%d",     syslog_level, 3),
  CVMFS_OPT("entry_timeout=%d",    entry_timeout, 60),
  CVMFS_OPT("attr_timeout=%d",     attr_timeout, 60),
  CVMFS_OPT("negative_timeout=%d", negative_timeout, 60),
  CVMFS_SWITCH("use_ino",          entry_timeout),
  CVMFS_SWITCH("kernel_cache",     kernel_cache),
  CVMFS_SWITCH("auto_cache",       auto_cache),

  FUSE_OPT_KEY("-V",            KEY_VERSION),
  FUSE_OPT_KEY("--version",     KEY_VERSION),
  FUSE_OPT_KEY("-h",            KEY_HELP),
  FUSE_OPT_KEY("--help",        KEY_HELP),
  FUSE_OPT_KEY("-f",            KEY_FOREGROUND),
  FUSE_OPT_KEY("-d",            KEY_DEBUG),
  FUSE_OPT_KEY("debug",         KEY_DEBUG),
  FUSE_OPT_KEY("-s",            KEY_SINGLETHREAD),
  {0, 0, 0},
};


CvmfsOptions g_cvmfs_opts;
struct fuse_args g_fuse_args;
bool g_foreground = false;
bool g_single_threaded = false;

/**
 * Display the usage message.
 * It will be done when we requested (the flag "-h" for example),
 * but also when an unidentified option is found.
 */
static void usage(const char *progname) {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Copyright (c) 2009- CERN\n"
           "All rights reserved\n\n"
           "Please visit http://cernvm.cern.ch details.\n\n");

  if (progname) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "usage: %s <mountpath> <url>[,<url>]* [options]\n\n", progname);
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
    "where options are:\n"
    " -o opt,[opt...]  mount options\n\n"
    "CernVM-FS options: \n"
    " -o timeout=SECONDS         "
      "Timeout for network operations (default is %d)\n"
    " -o timeout_direct=SECONDS  "
      "Timeout for network operations without proxy (default is %d)\n"
    " -o max_ttl=MINUTES         "
      "Maximum TTL for file catalogs (default: take from catalog)\n"
    " -o cachedir=DIR            Where to store disk cache\n"
    " -o proxies=HTTP_PROXIES    "
      "Set the HTTP proxy list, such as 'proxy1|proxy2;DIRECT'\n"
    " -o tracefile=FILE          Trace FUSE opaerations into FILE\n"
    " -o pubkey=PEMFILE          "
      "Public RSA key that is used to verify the whitelist signature.\n"
    " -o force_signing           "
      "Except only signed catalogs\n"
    " -o rebuild_cachedb         "
      "Force rebuilding the quota cache db from cache directory\n"
    " -o quota_limit=MB          "
      "Limit size of data chunks in cache. -1 Means unlimited.\n"
    " -o quota_threshold=MB      Cleanup until size is <= threshold\n"
    " -o nofiles=NUMBER          "
      "Set the maximum number of open files for CernVM-FS process "
      "(soft limit)\n"
    " -o grab_mountpoint         "
      "Give ownership of the mountpoint to the user before mounting "
      "(automount hack)\n"
    " -o logfile=FILE            "
      "Logs all messages to FILE instead of stderr and daemonizes.\n"
      "                            Makes only sense for the debug version\n"
    " -o repo_name=<repository>  "
      "Unique name of the mounted repository, e.g. atlas.cern.ch\n"
    " -o blacklist=FILE          "
      "Local blacklist for invalid certificates.  "
      "Has precedence over the whitelist.\n"
      "                            (Default is /etc/cvmfs/blacklist)\n"
    " -o syslog_level=NUMBER     "
      "Sets the level used for syslog to DEBUG (1), INFO (2), or NOTICE (3).\n"
    "                            Default is NOTICE.\n"
    " Note: you cannot load files greater than quota_limit-quota_threshold\n",
      2, 2);

  // Print the help from FUSE
  // const char *args[] = {progname, "-h"};
  // static struct fuse_operations op;
  // fuse_main(2, (char**)args, &op);
}


/**
 * Since certain fileds in cvmfs_opts are filled automatically when parsing it,
 * we need a procedure to free the space.
 */
static void FreeCvmfsOptions(CvmfsOptions *opts) {
  if (opts->hostname)       free(opts->hostname);
  if (opts->cachedir)       free(opts->cachedir);
  if (opts->proxies)        free(opts->proxies);
  if (opts->tracefile)      free(opts->tracefile);
  if (opts->pubkey)         free(opts->pubkey);
  if (opts->logfile)        free(opts->logfile);
  if (opts->blacklist)      free(opts->blacklist);
  if (opts->repo_name)      free(opts->repo_name);
  delete cvmfs::cachedir_;
  delete cvmfs::tracefile_;
  delete cvmfs::repository_name_;
  delete cvmfs::root_url_;
  delete cvmfs::mountpoint_;
  cvmfs::cachedir_ = NULL;
  cvmfs::tracefile_ = NULL;
  cvmfs::repository_name_ = NULL;
  cvmfs::root_url_ = NULL;
  cvmfs::mountpoint_ = NULL;
}


/**
 * Checks whether the given option is one of our own options
 * (if it's not, it probably belongs to fuse).
 */
static int IsCvmfsOption(const char *arg) {
  if (arg[0] != '-') {
    unsigned arglen = strlen(arg);
    const char **o;
    for (o = (const char**)cvmfs_array_opts; *o; o++) {
      unsigned olen = strlen(*o);
      if ((arglen > olen && arg[olen] == '=') &&
          (strncasecmp(arg, *o, olen) == 0))
        return 1;
    }
  }
  return 0;
}


/**
 * The callback used when fuse is parsing all the options
 * We separate CVMFS options from FUSE options here.
 *
 * \return On success zero, else non-zero
 */
static int ParseFuseOptions(void *data __attribute__((unused)), const char *arg,
                            int key, struct fuse_args *outargs) {
  switch (key) {
    case FUSE_OPT_KEY_OPT:
      if (IsCvmfsOption(arg)) {
        // If this is a "-o" option and is not one of ours, we assume that this
        // must be used for mounting fuse.
        // It can't be one of our option if it doesnt match the template.
        return 0;
      }
      if (strstr(arg, "uid=")) {
        cvmfs::uid_ = atoi(arg+4);
        return 0;
      }
      if (strstr(arg, "gid=")) {
        cvmfs::gid_ = atoi(arg+4);
        return 0;
      }
      return 1;

    case FUSE_OPT_KEY_NONOPT:
      if (!g_cvmfs_opts.hostname &&
          ((strstr(arg, "http://") == arg) ||
           (strstr(arg, "file://") == arg))) {
        // If we receive a parameter that contains "http://"
        // we know for sure that it's our remote server
        g_cvmfs_opts.hostname = strdup(arg);
      } else {
        // If we receive any other string, we take it as the mount point.
        cvmfs::mountpoint_ = new string(arg);
      }
      return 0;

    case KEY_HELP:
      usage(outargs->argv[0]);
      fuse_opt_add_arg(outargs, "-ho");
      exit(0);

    case KEY_VERSION:
      LogCvmfs(kLogCvmfs, kLogStderr, "CernVM-FS version %s\n",
               PACKAGE_VERSION);
#if FUSE_VERSION >= 25
      fuse_opt_add_arg(outargs, "--version");
#endif
      exit(0);

    case KEY_FOREGROUND:
      g_foreground = true;
      return 0;
    case KEY_SINGLETHREAD:
      g_single_threaded = true;
      return 0;
    case KEY_DEBUG:
      fuse_opt_add_arg(outargs, "-d");
      return 0;
    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "internal option parsing error");
      abort();
  }
}


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

static pthread_t CallbackLibcryptoThreadId() {
  return pthread_self();
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


/**
 * Off we go
 */
int main(int argc, char *argv[]) {
  int retval;
  int result = -1;
  struct fuse_chan *ch;
  int fd_lockfile = -1;
  void *sqlite_scratch = NULL;
  void *sqlite_page_cache = NULL;
  bool options_ready = false;
  bool download_ready = false;
  bool cache_ready = false;
  bool monitor_ready = false;
  bool signature_ready = false;
  bool quota_ready = false;
  bool catalog_ready = false;
  bool talk_ready = false;
  bool running_created = false;

  // Set a decent umask for new files (no write access to group/everyone).
  // We want to allow group write access for the talk-socket.
  umask(007);

  cvmfs::boot_time_ = time(NULL);
  SetupLibcryptoMt();

  // Parse options
  g_fuse_args.argc = argc;
  g_fuse_args.argv = argv;
  g_fuse_args.allocated = 0;
  if ((fuse_opt_parse(&g_fuse_args, &g_cvmfs_opts, cvmfs_array_opts,
                      ParseFuseOptions) != 0) ||
      !g_cvmfs_opts.hostname) {
    usage(argv[0]);
    goto cvmfs_cleanup;
    return 1;
  }

  // Default options
  if (g_cvmfs_opts.timeout == 0) g_cvmfs_opts.timeout = 2;
  if (g_cvmfs_opts.timeout_direct == 0) g_cvmfs_opts.timeout_direct = 2;
  if (g_cvmfs_opts.syslog_level == 0) g_cvmfs_opts.syslog_level = 3;
  if (!g_cvmfs_opts.tracefile) g_cvmfs_opts.tracefile = strdup("");
  if (!g_cvmfs_opts.blacklist) g_cvmfs_opts.blacklist = strdup("");
  if (!g_cvmfs_opts.repo_name) g_cvmfs_opts.repo_name = strdup("");
  if (!g_cvmfs_opts.cachedir)
    g_cvmfs_opts.cachedir = strdup("/var/lib/cvmfs/default");

  // Fill cvmfs option variables from Fuse options
  cvmfs::cachedir_ = new string(g_cvmfs_opts.cachedir);
  cvmfs::tracefile_ = new string(g_cvmfs_opts.tracefile);
  cvmfs::repository_name_ = new string(g_cvmfs_opts.repo_name);
  if (!cvmfs::uid_) cvmfs::uid_ = getuid();
  if (!cvmfs::gid_) cvmfs::gid_ = getgid();
  if (g_cvmfs_opts.max_ttl) cvmfs::max_ttl_ = g_cvmfs_opts.max_ttl*60;

  // Seperate first host from hostlist
  unsigned iter_hostname;
  for (iter_hostname = 0; iter_hostname < strlen(g_cvmfs_opts.hostname);
       ++iter_hostname)
  {
    if (g_cvmfs_opts.hostname[iter_hostname] == ',') break;
  }
  if (iter_hostname == 0)
    cvmfs::root_url_ = new string("");
  else
    cvmfs::root_url_ = new string(g_cvmfs_opts.hostname, iter_hostname);
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
  atomic_init64(&cvmfs::num_open_);
  atomic_init64(&cvmfs::num_download_);
  atomic_init32(&cvmfs::num_io_error_);
  cvmfs::previous_io_error_.timestamp = 0;
  cvmfs::previous_io_error_.delay = 0;

  // Logging
  SetLogSyslogLevel(g_cvmfs_opts.syslog_level);
  SetLogSyslogPrefix(*cvmfs::repository_name_);
  if (g_cvmfs_opts.logfile)
    SetLogDebugFile(string(g_cvmfs_opts.logfile));

  // Maximum number of open files
  if (g_cvmfs_opts.nofiles) {
    if (g_cvmfs_opts.nofiles < 0) {
      PrintError("number of open files must be a positive number");
      goto cvmfs_cleanup;
    }
    struct rlimit rpl;
    memset(&rpl, 0, sizeof(rpl));
    getrlimit(RLIMIT_NOFILE, &rpl);
    if (rpl.rlim_max < (unsigned)g_cvmfs_opts.nofiles)
      rpl.rlim_max = g_cvmfs_opts.nofiles;
    rpl.rlim_cur = g_cvmfs_opts.nofiles;
    if (setrlimit(RLIMIT_NOFILE, &rpl) != 0) {
      PrintError("Failed to set maximum number of open files, "
                 "insufficient permissions");
      goto cvmfs_cleanup;
    }
  }

  // Grab mountpoint
  if (g_cvmfs_opts.grab_mountpoint) {
    if ((chown(cvmfs::mountpoint_->c_str(), cvmfs::uid_, cvmfs::gid_) != 0) ||
        (chmod(cvmfs::mountpoint_->c_str(), 0755) != 0)) {
      PrintError("Failed to grab mountpoint (" + StringifyInt(errno) + ")");
      goto cvmfs_cleanup;
    }
  }

  // Drop credentials
  if ((cvmfs::uid_ != 0) || (cvmfs::gid_ != 0)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: running with credentials %d:%d",
             cvmfs::uid_, cvmfs::gid_);
    if ((setgid(cvmfs::gid_) != 0) || (setuid(cvmfs::uid_) != 0)) {
      PrintError("Failed to drop credentials");
      goto cvmfs_cleanup;
    }
  }

  // Try to jump to cache directory.  This tests, if it is accassible.
  // Also, it brings speed later on.
  if (!MkdirDeep(*cvmfs::cachedir_, 0700)) {
    PrintError("cannot create cache directory " + *cvmfs::cachedir_);
    goto cvmfs_cleanup;
  }
  if (chdir(cvmfs::cachedir_->c_str()) != 0) {
    PrintError("cache directory " + *cvmfs::cachedir_ + " is unavailable");
    goto cvmfs_cleanup;
  }

  // Create lock file
  fd_lockfile = open("lock", O_RDONLY | O_CREAT, 0600);
  if (fd_lockfile < 0) {
    PrintError("could not open lock file (" + StringifyInt(errno) + ")");
    goto cvmfs_cleanup;
  }
  if (flock(fd_lockfile, LOCK_EX) != 0) {
    PrintError("could not acquire lock (" + StringifyInt(errno) + ")");
    goto cvmfs_cleanup;
  }
  {
    platform_stat64 info;
    if (platform_stat("running", &info) == 0) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "looks like cvmfs has been "
               "crashed previously, rebuilding cache database");
      g_cvmfs_opts.rebuild_cachedb = 1;
    }
  }
  retval = open("running", O_RDONLY | O_CREAT, 0600);
  if (retval < 0) {
    PrintError("could not open running sentinel (" + StringifyInt(errno) + ")");
    goto cvmfs_cleanup;
  }
  close(retval);
  running_created = true;

  // Creates a set of cache directories (256 directories named 00..ff)
  if (!cache::Init(".", *cvmfs::root_url_)) {
    PrintError("Failed to setup cache in " + *cvmfs::cachedir_ +
               ": " + strerror(errno));
    goto cvmfs_cleanup;
  }
  cache_ready = true;

  // Init quota / managed cache
  if (g_cvmfs_opts.quota_limit < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "unlimited cache size");
    g_cvmfs_opts.quota_limit = -1;
    g_cvmfs_opts.quota_threshold = 0;
  } else {
    g_cvmfs_opts.quota_limit *= 1024*1024;
    g_cvmfs_opts.quota_threshold *= 1024*1024;
  }
  if (!quota::Init(".", (uint64_t)g_cvmfs_opts.quota_limit,
                 (uint64_t)g_cvmfs_opts.quota_threshold,
                 g_cvmfs_opts.rebuild_cachedb)) {
    PrintError("Failed to initialize lru cache");
    goto cvmfs_cleanup;
  }
  quota_ready = true;

  if (g_cvmfs_opts.rebuild_cachedb) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: rebuilding lru cache database...");
    if (!quota::RebuildDatabase()) {
      PrintError("Failed to rebuild lru cache database");
      goto cvmfs_cleanup;
    }
  }
  if (quota::GetSize() > quota::GetCapacity()) {
    PrintWarning("your cache is already beyond quota size, cleaning up");
    if (!quota::Cleanup(g_cvmfs_opts.quota_threshold)) {
      PrintWarning("Failed to clean up");
      goto cvmfs_cleanup;
    }
  }
  if (g_cvmfs_opts.quota_limit) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: quota initialized, current size %luMB",
             quota::GetSize()/(1024*1024));
  }

  // Monitor, check for maximum number of open files
  if (!monitor::Init(".", true)) {
    PrintError("failed to initialize watchdog.");
    goto cvmfs_cleanup;
  }
  cvmfs::max_open_files_ = monitor::GetMaxOpenFiles();
  atomic_init32(&cvmfs::open_files_);
  monitor_ready = true;

  // Control & command interface
  if (!talk::Init(".")) {
    PrintError("failed to initialize talk socket (" + StringifyInt(errno) +
               ")");
    goto cvmfs_cleanup;
  }
  talk_ready = true;

  // Network initialization
  download::Init(16);
  download::SetHostChain(string(g_cvmfs_opts.hostname));
  download::SetProxyChain(g_cvmfs_opts.proxies ?
                          string(g_cvmfs_opts.proxies) : "");
  download::SetTimeout(g_cvmfs_opts.timeout, g_cvmfs_opts.timeout_direct);
  download_ready = true;

  signature::Init();
  if (!signature::LoadPublicRsaKeys(g_cvmfs_opts.pubkey ?
                                    g_cvmfs_opts.pubkey : ""))
  {
    PrintError("failed to load public key(s)");
    goto cvmfs_cleanup;
  } else {
    if (!g_cvmfs_opts.pubkey)
      PrintWarning("No public master key given. "
                   "Cvmfs will fail on signed catalogs!");
    else
      LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: using public key(s) %s",
               JoinStrings(
                 SplitString(g_cvmfs_opts.pubkey, ':'), ", ").c_str());
  }
  signature_ready = true;

  // Load initial file catalog
  cvmfs::catalog_manager_ = new
    cvmfs::RemoteCatalogManager(*cvmfs::root_url_, *cvmfs::repository_name_,
      "/.cvmfswhitelist", g_cvmfs_opts.blacklist, g_cvmfs_opts.force_signing);
  if (not cvmfs::catalog_manager_->Init()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to initialize catalog manager");
    goto cvmfs_cleanup;
  }
  catalog_ready = true;

  // Set fuse callbacks, remove url from arguments
  LogCvmfs(kLogCvmfs, kLogSyslog,
           "CernVM-FS: linking %s to remote directoy %s",
           cvmfs::mountpoint_->c_str(), cvmfs::root_url_->c_str());
  struct fuse_lowlevel_ops cvmfs_operations;
  cvmfs::set_cvmfs_ops(&cvmfs_operations);

  cvmfs::inode_cache_ = new cvmfs::InodeCache(cvmfs::kInodeCacheSize);
  cvmfs::path_cache_ = new cvmfs::PathCache(cvmfs::kPathCacheSize);
  cvmfs::md5path_cache_ = new cvmfs::Md5PathCache(cvmfs::kMd5pathCacheSize);
  cvmfs::directory_handles_ = new map<uint64_t, cvmfs::DirectoryListing>();

  if ((ch = fuse_mount(cvmfs::mountpoint_->c_str(), &g_fuse_args)) != NULL) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: mounted cvmfs on %s",
             cvmfs::mountpoint_->c_str());
    if (!g_foreground)
      daemon(0, 0);

    struct fuse_session *se;
    se = fuse_lowlevel_new(&g_fuse_args, &cvmfs_operations,
                           sizeof(cvmfs_operations), NULL);
    if (se != NULL) {
      if (fuse_set_signal_handlers(se) != -1) {
        fuse_session_add_chan(se, ch);
        if (g_single_threaded)
          result = fuse_session_loop(se);
        else
          result = fuse_session_loop_mt(se);
        fuse_remove_signal_handlers(se);
        fuse_session_remove_chan(ch);
      }
      fuse_session_destroy(se);
    }
    fuse_unmount(cvmfs::mountpoint_->c_str(), ch);
  }
  fuse_opt_free_args(&g_fuse_args);

  delete cvmfs::catalog_manager_;
  delete cvmfs::directory_handles_;
  delete cvmfs::path_cache_;
  delete cvmfs::inode_cache_;
  delete cvmfs::md5path_cache_;
  cvmfs::catalog_manager_ = NULL;
  cvmfs::directory_handles_ = NULL;
  cvmfs::path_cache_ = NULL;
  cvmfs::inode_cache_ = NULL;
  cvmfs::md5path_cache_ = NULL;

  LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "CernVM-FS: unmounted %s (%s)",
           cvmfs::mountpoint_->c_str(), cvmfs::root_url_->c_str());

 cvmfs_cleanup:
  if (signature_ready) signature::Fini();
  if (download_ready) download::Fini();
  if (talk_ready) talk::Fini();
  if (monitor_ready) monitor::Fini();
  if (quota_ready) quota::Fini();
  if (cache_ready) cache::Fini();
  if (running_created) unlink("running");
  if (fd_lockfile >= 0) {
    int retval = flock(fd_lockfile, LOCK_UN);
    assert(retval == 0);
    close(fd_lockfile);
  }
  if (options_ready) {
    fuse_opt_free_args(&g_fuse_args);
    FreeCvmfsOptions(&g_cvmfs_opts);
  }

  if (sqlite_page_cache) free(sqlite_page_cache);
  if (sqlite_scratch) free(sqlite_scratch);

  CleanupLibcryptoMt();

  return result;
}
