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
#include "cache_posix.h"
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
#include "murmur.h"
#include "platform.h"
#include "quota.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "sqlitemem.h"
#include "sqlitevfs.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

// TODO(jblomer): remove.  Only needed to satisfy monitor.cc
namespace cvmfs {
  pid_t pid_ = 0;
}


LibGlobals* LibGlobals::instance_ = NULL;
LibGlobals* LibGlobals::GetInstance() {
  assert(LibGlobals::instance_ != NULL);
  return LibGlobals::instance_;
}


/**
 * Always creates the singleton, even in case of failure.
 */
loader::Failures LibGlobals::Initialize(OptionsManager *options_mgr) {
  LogCvmfs(kLogCvmfs, kLogStdout, "LibCvmfs version %d.%d, revision %d",
           LIBCVMFS_VERSION_MAJOR, LIBCVMFS_VERSION_MINOR, LIBCVMFS_REVISION);

  assert(options_mgr != NULL);
  assert(instance_ == NULL);
  instance_ = new LibGlobals();
  assert(instance_ != NULL);

  // Multi-threaded libcrypto (otherwise done by the loader)
  instance_->libcrypto_locks_ = static_cast<pthread_mutex_t *>(
    OPENSSL_malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t)));
  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    int retval = pthread_mutex_init(&(instance_->libcrypto_locks_[i]), NULL);
    assert(retval == 0);
  }
  CRYPTO_set_id_callback(LibGlobals::CallbackLibcryptoThreadId);
  CRYPTO_set_locking_callback(LibGlobals::CallbackLibcryptoLock);

  FileSystem::FileSystemInfo fs_info;
  fs_info.name = "libcvmfs";
  fs_info.type = FileSystem::kFsLibrary;
  fs_info.options_mgr = options_mgr;
  instance_->file_system_ = FileSystem::Create(fs_info);

  if (instance_->file_system_->boot_status() != loader::kFailOk)
    return instance_->file_system_->boot_status();

  // Maximum number of open files, handled otherwise as root by the fuse loader
  string arg;
  if (options_mgr->GetValue("CVMFS_NFILES", &arg)) {
    int retval = SetLimitNoFile(String2Uint64(arg));
    if (retval != 0) {
      PrintError("Failed to set maximum number of open files, "
                 "insufficient permissions");
      return loader::kFailPermission;
    }
  }

  return loader::kFailOk;
}


void LibGlobals::CleanupInstance() {
  if (instance_ != NULL) {
    delete instance_;
    instance_ = NULL;
  }
  assert(instance_ == NULL);
}


LibGlobals::LibGlobals()
  : options_mgr_(NULL)
  , file_system_(NULL)
  , libcrypto_locks_(NULL)
{ }


LibGlobals::~LibGlobals() {
  delete file_system_;
  delete options_mgr_;

  if (libcrypto_locks_) {
    CRYPTO_set_locking_callback(NULL);
    for (int i = 0; i < CRYPTO_num_locks(); ++i)
      pthread_mutex_destroy(&(libcrypto_locks_[i]));
    OPENSSL_free(libcrypto_locks_);
  }
}


void LibGlobals::CallbackLibcryptoLock(
  int mode,
  int type,
  const char *file,
  int line)
{
  (void)file;
  (void)line;

  int retval;
  LibGlobals *globals = LibGlobals::GetInstance();
  pthread_mutex_t *locks = globals->libcrypto_locks_;
  pthread_mutex_t *lock = &(locks[type]);

  if (mode & CRYPTO_LOCK) {
    retval = pthread_mutex_lock(lock);
  } else {
    retval = pthread_mutex_unlock(lock);
  }
  assert(retval == 0);
}


// Type unsigned long required by libcrypto (openssl)
unsigned long LibGlobals::CallbackLibcryptoThreadId() {  // NOLINT
  return platform_gettid();
}


//------------------------------------------------------------------------------


LibContext *LibContext::Create(
  const string &fqrn,
  OptionsManager *options_mgr)
{
  assert(options_mgr != NULL);
  LibContext *ctx = new LibContext();
  assert(ctx != NULL);

  ctx->mount_point_ = MountPoint::Create(
    fqrn, LibGlobals::GetInstance()->file_system(), options_mgr);
  return ctx;
}


LibContext::LibContext()
  : options_mgr_(NULL)
  , mount_point_(NULL)
{ }


LibContext::~LibContext() {
  delete mount_point_;
  delete options_mgr_;
}


bool LibContext::GetDirentForPath(const PathString         &path,
                                  catalog::DirectoryEntry  *dirent)
{
  if (path.GetLength() == 1 && path.GetChars()[0] == '/') {
    // root path is expected to be "", not "/"
    PathString p;
    return GetDirentForPath(p, dirent);
  }
  shash::Md5 md5path(path.GetChars(), path.GetLength());
  if (mount_point_->md5path_cache()->Lookup(md5path, dirent))
    return dirent->GetSpecial() != catalog::kDirentNegative;

  // TODO(jblomer): not twice md5 calculation
  if (mount_point_->catalog_mgr()->LookupPath(path, catalog::kLookupSole,
                                              dirent))
  {
    mount_point_->md5path_cache()->Insert(md5path, *dirent);
    return true;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForPath, no entry");
  // Only cache real ENOENT errors, not catalog load errors
  if (dirent->GetSpecial() == catalog::kDirentNegative)
    mount_point_->md5path_cache()->InsertNegative(md5path);

  return false;
}


void LibContext::AppendStringToList(char const   *str,
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


int LibContext::GetAttr(const char *c_path, struct stat *info) {
  perf::Inc(file_system()->n_fs_stat());
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


int LibContext::Readlink(const char *c_path, char *buf, size_t size) {
  perf::Inc(file_system()->n_fs_readlink());
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


int LibContext::ListDirectory(
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
  if (d.inode() != mount_point_->catalog_mgr()->GetRootInode()) {
    AppendStringToList("..", buf, &listlen, buflen);
  }

  // Add all names
  catalog::StatEntryList listing_from_catalog;
  if (!mount_point_->catalog_mgr()->ListingStat(path, &listing_from_catalog)) {
    return -EIO;
  }
  for (unsigned i = 0; i < listing_from_catalog.size(); ++i) {
    AppendStringToList(listing_from_catalog.AtPtr(i)->name.c_str(),
                          buf, &listlen, buflen);
  }

  return 0;
}


int LibContext::Open(const char *c_path) {
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
    if (mount_point_->catalog_mgr()->ListFileChunks(
          path, dirent.hash_algorithm(), chunks) ||
        chunks->IsEmpty())
    {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr, "file %s is marked as "
               "'chunked', but no chunks found.", path.c_str());
      perf::Inc(file_system()->n_io_error());
      delete chunks;
      return -EIO;
    }

    fd = mount_point_->simple_chunk_tables()->Add(
      FileChunkReflist(chunks, path, dirent.compression_algorithm(),
                       dirent.IsExternalFile()));
    return fd | kFdChunked;
  }

  cvmfs::Fetcher *this_fetcher = dirent.IsExternalFile()
    ? mount_point_->external_fetcher()
    : mount_point_->fetcher();
  fd = this_fetcher->Fetch(
    dirent.checksum(),
    dirent.size(),
    string(path.GetChars(), path.GetLength()),
    dirent.compression_algorithm(),
    cache::CacheManager::kTypeRegular);
  perf::Inc(file_system()->n_fs_open());

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

  perf::Inc(file_system()->n_io_error());
  return fd;
}


int64_t LibContext::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t off)
{
  if (fd & kFdChunked) {
    ClientCtxGuard ctxg(geteuid(), getegid(), getpid());
    const int chunk_handle = fd & ~kFdChunked;
    SimpleChunkTables::OpenChunks open_chunks =
      mount_point_->simple_chunk_tables()->Get(chunk_handle);
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
        if (chunk_fd->fd != -1) file_system()->cache_mgr()->Close(chunk_fd->fd);
        if (open_chunks.chunk_reflist.external_data) {
          chunk_fd->fd = mount_point_->external_fetcher()->Fetch(
            chunk_list->AtPtr(chunk_idx)->content_hash(),
            chunk_list->AtPtr(chunk_idx)->size(),
            "no path info",
            compression_alg,
            cache::CacheManager::kTypeRegular,
            open_chunks.chunk_reflist.path.ToString(),
            chunk_list->AtPtr(chunk_idx)->offset());
        } else {
          chunk_fd->fd = mount_point_->fetcher()->Fetch(
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
      const int64_t bytes_fetched = file_system()->cache_mgr()->Pread(
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
    return file_system()->cache_mgr()->Pread(fd, buf, size, off);
  }
}


int LibContext::Close(int fd) {
  LogCvmfs(kLogCvmfs, kLogDebug, "cvmfs_close on file number: %d", fd);
  if (fd & kFdChunked) {
    const int chunk_handle = fd & ~kFdChunked;
    SimpleChunkTables::OpenChunks open_chunks =
      mount_point_->simple_chunk_tables()->Get(chunk_handle);
    if (open_chunks.chunk_reflist.list == NULL)
      return -EBADF;
    if (open_chunks.chunk_fd->fd != -1)
      file_system()->cache_mgr()->Close(open_chunks.chunk_fd->fd);
    mount_point_->simple_chunk_tables()->Release(chunk_handle);
  } else {
    file_system()->cache_mgr()->Close(fd);
  }
  return 0;
}


catalog::LoadError cvmfs_context::RemountStart() {
  return catalog::kLoadNew;
}
