/**
 * This file is part of the CernVM File System.
 *
 * The cache module maintains the local file cache.  Files are
 * staged into the cache by Fetch().  The cache stores files with a name
 * according to their content hash.
 *
 * The procedure is
 *   -# Look in the catalog for SHA1 hash
 *   -# If it is in local cache: return file descriptor
 *   -# Otherwise download, store in cache and return fd
 *
 * Each running CVMFS instance has to have a separate cache directory.
 * The local cache directory (directories 00..ff) can be accessed
 * in parallel to a running CVMFS, i.e. files can be deleted for instance
 * anytime.  However, this will confuse the cache database managed by the lru
 * module.
 *
 * Files are created in txn directory first.  At the very latest
 * point they are renamed into their "real" content hash names atomically by
 * rename().  This concept is taken over from GROW-FS.
 *
 * Identical URLs won't be concurrently downloaded.  The first thread performs
 * the download and informs the other, waiting threads on pipes.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "cache.h"

#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>
#include <inttypes.h>

#include <cassert>
#include <cstring>
#include <cstdlib>
#include <cstdio>

#include <map>
#include <vector>

#include "platform.h"
#include "dirent.h"
#include "quota.h"
#include "util.h"
#include "hash.h"
#include "logging.h"
#include "download.h"
#include "compression.h"
#include "smalloc.h"
#include "signature.h"
#include "atomic.h"
#include "shortstring.h"

using namespace std;  // NOLINT

namespace cache {

/**
 * Everything that should be reused per thread
 */
struct ThreadLocalStorage {
  int pipe_wait[2];
  vector<int> other_pipes_waiting;
  download::JobInfo download_job;
};

typedef map< hash::Any, vector<int> * > ThreadQueues;

string *cache_path_ = NULL;
ThreadQueues *queues_download_ = NULL;  /**< maps currently
  downloaded chunks to an array of writer's ends of a pipe to signal the waiting
  threads when the download has finished */
pthread_mutex_t lock_queues_download_ = PTHREAD_MUTEX_INITIALIZER;
pthread_key_t thread_local_storage_;
atomic_int64 num_download_;


static void CleanupTLS(void *data) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(data);
  close(tls->pipe_wait[0]);
  close(tls->pipe_wait[1]);
  delete tls;
}

/**
 * Initializes the cache directory with the 256 subdirectories and /txn.
 *
 * \return True on success, false otherwise
 */
bool Init(const string &cache_path) {
  cache_path_ = new string(cache_path);
  queues_download_ = new ThreadQueues();
  atomic_init64(&num_download_);

  if (!MakeCacheDirectories(cache_path, 0700))
    return false;

  // Cleanup dangling checksums
  DIR *dirp = NULL;
  platform_dirent64 *d;
  if ((dirp = opendir(cache_path.c_str())) == NULL) {
    LogCvmfs(kLogCache, kLogDebug, "failed to open directory %s",
             cache_path.c_str());
    return false;
  }
  while ((d = platform_readdir(dirp)) != NULL) {
    if (d->d_type != DT_REG) continue;

    const string name(d->d_name);
    if (name.substr(0, 14) == "cvmfs.checksum") {
      const string current_path = cache_path + "/" + name;
      FILE *f = fopen(current_path.c_str(), "r");
      if (f != NULL) {
        char sha1[40];
        if (fread(sha1, 1, 40, f) == 40) {
          const string sha1_str = string(sha1, 40);
          LogCvmfs(kLogCache, kLogDebug, "found checksum %s", sha1_str.c_str());
          const string sha1_path = cache_path + "/" + sha1_str.substr(0,2) +
                                   "/" + sha1_str.substr(2);

          if (!FileExists(sha1_path))
            unlink(current_path.c_str());
        } else {
          unlink(current_path.c_str());
        }
        fclose(f);
      } else {
        closedir(dirp);
        return false;
      }
    }
  }
  closedir(dirp);

  int retval = pthread_key_create(&thread_local_storage_, CleanupTLS);
  assert(retval == 0);

  return true;
}


void Fini() {
  // TODO: wait for file transfers to finish
  // (they are canceled by finilizing the download thread)
  pthread_key_delete(thread_local_storage_);
  delete cache_path_;
  delete queues_download_;
  cache_path_ = NULL;
  queues_download_ = NULL;
}


/**
 * Transforms a catalog entry into a name for local cache.
 *
 * @param[in] id content hash of the catalog entry.
 * \return Absolute path in local cache.
 */
static inline string GetPathInCache(const hash::Any &id) {
  return *cache_path_ + id.MakePath(1, 2);
}


/**
 * Transform a catalog entry into a temporary name in txn-directory.
 *
 * @param[in] id content hash of the catalog entry.
 * \return Absolute path in local cache txn-directory.
 */
static inline string GetTempName()
{
  return *cache_path_ + "/txn/" + "fetchXXXXXX";
}


/**
 * Tries to open a catalog entry in local cache.
 *
 * @param[in] id content hash of the catalog entry.
 * \return A file descriptor if file is in cache.  Error code of open() else.
 */
int Open(const hash::Any &id) {
  const string path = GetPathInCache(id);
  int result = ::open(path.c_str(), O_RDONLY);

  if (result >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit %s", path.c_str());
    platform_disable_kcache(result);
  } else {
    result = -errno;
    LogCvmfs(kLogCache, kLogDebug, "miss %s (%d)", path.c_str(), result);
  }

  return result;
}


/**
 * Tries to open a catalog and copies its contents into a newly malloced
 * memory area.  User of the function has to free buffer (if successful).
 *
 * @param[in] id content hash of the catalog entry.
 * @param[out] buffer Contents of the file
 * @param[out] size Size of the file
 * \return True if successful, false otherwise.
 */
bool Open2Mem(const hash::Any &id, unsigned char **buffer, uint64_t *size) {
  *size = 0;
  *buffer = NULL;

  int fd = cache::Open(id);
  if (fd < 0)
    return false;

  platform_stat64 info;
  if (platform_fstat(fd, &info) != 0) {
    close(fd);
    return false;
  }

  *size = info.st_size;
  *buffer = static_cast<unsigned char *>(smalloc(*size));

  int64_t retval = read(fd, *buffer, *size);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != *size)) {
    close(fd);
    free(*buffer);
    *buffer = NULL;
    *size = 0;
    return false;
  }

  close(fd);
  return true;
}


/**
 * Starts a "transaction" based on a catalog entry, i.e. start download in a
 * temporary file.
 *
 * @param[in] id content hash of the catalog entry.
 * @param[out] path Absolute path of the file in local cache after commit
 * @param[out] temp_path Absolute path of the temporoary file in local cache
 * \return File descriptor of temporary file, error code of mkstemp() else
 */
int StartTransaction(const hash::Any &id,
                     string *final_path, string *temp_path)
{
  int result;
  *final_path = GetPathInCache(id);
  *temp_path = GetTempName();
  const unsigned temp_path_length = temp_path->length();

  char template_path[temp_path_length + 1];
  memcpy(template_path, &(*temp_path)[0], temp_path_length);
  template_path[temp_path_length] = '\0';
  result = ::mkstemp(template_path);
  if (result == -1)
    result = -errno;

  LogCvmfs(kLogCache, kLogDebug, "start transaction on %s has result %d",
           template_path, result);

  *temp_path = template_path;
  return result;
}


/**
 * Aborts a file download started with StartTransaction() and cleans
 * temporoary storage.
 *
 * @param[in] temp_path Absolute path of the temporoary file in local cache
 * \return Zero on success, error code of unlink() else.
 */
int AbortTransaction(const string &temp_path) {
  LogCvmfs(kLogCache, kLogDebug, "abort %s", temp_path.c_str());
  int result = unlink(temp_path.c_str());
  if (result == -1)
    return -errno;
  return result;
}


/**
 * Commits a file download started with StartTransaction(), i.e. renames
 * the temporary file to its real content hash name.
 *
 * If the cache is managed (quota / lru), it also inserts the file into
 * the lru database.
 *
 * @param[in] final_path Absolute content hash path of the file in local cache
 * @param[in] temp_path Absolute path of the temporoary file in local cache
 * @param[in] cvmfs_path Path of the chunk as seen in cvmfs
 * @param[in] hash Content hash of the file
 * @param[in] size Decompressed size of the file
 * \return Zero on success, non-zero else.
 */
int CommitTransaction(const string &final_path,
                      const string &temp_path,
                      const string &cvmfs_path,
                      const hash::Any &hash,
                      const uint64_t size)
{
  int result;
  LogCvmfs(kLogCache, kLogDebug, "commit %s %s",
           final_path.c_str(), temp_path.c_str());

  result = rename(temp_path.c_str(), final_path.c_str());
  if (result < 0) {
    result = -errno;
    LogCvmfs(kLogCache, kLogDebug, "commit failed: %s", strerror(errno));
    unlink(temp_path.c_str());
  } else {
    quota::Insert(hash, size, cvmfs_path);
  }

  return result;
}


/**
 * Commits the memory blob buffer to the given chunk id and name on cvmfs.
 * No checking! The hash and the memory blob need to match.
 */
bool CommitFromMem(const hash::Any &id, const unsigned char *buffer,
                   const uint64_t size, const std::string &cvmfs_path)
{
  string temp_path;
  string final_path;

  int fd = StartTransaction(id, &final_path, &temp_path);
  if (fd < 0)
    return false;

  ssize_t retval = write(fd, buffer, size);
  close(fd);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != size)) {
    AbortTransaction(temp_path);
    return false;
  }

  return CommitTransaction(final_path, temp_path, cvmfs_path, id, size) == 0;
}


/**
 * Checks for a file in local cache.  Because we support parallel operations,
 * this is just a hint.  Open gives a definitive answer.
 *
 * @param[in] id content hash of the catalog entry.
 * \return True, if file is in local cache, false otherwise.
 */
bool Contains(const hash::Any &id) {
  platform_stat64 info;
  return platform_stat(GetPathInCache(id).c_str(), &info) == 0;
}


/**
 * Returns a read-only file descriptor for a specific catalog entry.
 * After successful call, the file resides in local cache.
 * File is downloaded via HTTP if it is not in the local cache.
 * If multiple concurrent requests arrive for a file, the requests are queued
 * and only the first one performs the download.
 *
 * @param[in] d Demanded catalog entry
 * @param[in] cvmfs_path Path of the chunk as seen in cvmfs
 * \return Read-only file descriptor for the file pointing into local cache.
 *         On failure a negative error code.
 */
int Fetch(const catalog::DirectoryEntry &d, const string &cvmfs_path)
{
  int fd_return;  // Read-only file descriptor that is returned
  int retval;

  if (d.size() > quota::GetMaxFileSize()) {
    LogCvmfs(kLogCache, kLogDebug, "file too big for lru cache (%"PRIu64")",
             d.size());
    return -ENOSPC;
  }

  // Try to open from local cache
  if ((fd_return = cache::Open(d.checksum())) >= 0) {
    quota::Touch(d.checksum());
    return fd_return;
  }

  // Initialize TLS
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
                            pthread_getspecific(thread_local_storage_));
  if (tls == NULL) {
    tls = new ThreadLocalStorage();
    retval = pipe(tls->pipe_wait);
    assert(retval == 0);
    tls->download_job.destination = download::kDestinationFile;
    tls->download_job.compressed = true;
    tls->download_job.probe_hosts = true;
    retval = pthread_setspecific(thread_local_storage_, tls);
    assert(retval == 0);
  }

  // Lock queue and start downloading or enqueue
  pthread_mutex_lock(&lock_queues_download_);
  ThreadQueues::iterator iDownloadQueue = queues_download_->find(d.checksum());
  if (iDownloadQueue != queues_download_->end()) {
    LogCvmfs(kLogCache, kLogDebug, "waiting for download of %s",
             cvmfs_path.c_str());

    iDownloadQueue->second->push_back(tls->pipe_wait[1]);
    pthread_mutex_unlock(&lock_queues_download_);
    retval = read(tls->pipe_wait[0], &fd_return, sizeof(int));
    assert(retval = sizeof(int));

    LogCvmfs(kLogCache, kLogDebug, "received from another thread fd %d for %s",
             fd_return, cvmfs_path.c_str());
    return fd_return;
  } else {
    // Seems we are the first one, check again in the cache (race condition)
    fd_return = cache::Open(d.checksum());
    if (fd_return >= 0) {
      pthread_mutex_unlock(&lock_queues_download_);
      quota::Touch(d.checksum());
      return fd_return;
    }

    // Create a new queue for this chunk
    (*queues_download_)[d.checksum()] = &tls->other_pipes_waiting;
    pthread_mutex_unlock(&lock_queues_download_);
  }

  // The download path starts here
  LogCvmfs(kLogCache, kLogDebug, "downloading %s", cvmfs_path.c_str());
  atomic_inc64(&num_download_);

  const string url = "/data" + d.checksum().MakePath(1, 2);
  string final_path;
  string temp_path;
  int fd;  // Used to write the downloaded file
  FILE *f = NULL;
  int result = -EIO;

  fd = StartTransaction(d.checksum(), &final_path, &temp_path);
  if (fd < 0) {
    LogCvmfs(kLogCache, kLogDebug, "could not start transaction on %s",
             final_path.c_str());
    result = fd;
    goto fetch_finalize;
  }

  f = fdopen(fd, "w");
  if (!f) {
    result = -errno;
    LogCvmfs(kLogCache, kLogDebug, "could not fdopen %s", final_path.c_str());
    goto fetch_finalize;
  }

  tls->download_job.url = &url;
  tls->download_job.destination_file = f;
  tls->download_job.expected_hash = d.checksum_ptr();
  download::Fetch(&tls->download_job);

  if (tls->download_job.error_code == download::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug, "finished downloading of %s", url.c_str());

    // Check decompressed size (a cross check just in case)
    platform_stat64 stat_info;
    stat_info.st_size = -1;
    if ((platform_fstat(fileno(f), &stat_info) != 0) ||
        (stat_info.st_size != (int64_t)d.size()))
    {
      LogCvmfs(kLogCache, kLogSyslog,
               "size check failure for %s, expected %lu, got %ld",
               url.c_str(), d.size(), stat_info.st_size);
      if (CopyPath2Path(temp_path, *cache_path_ + "/quarantaine/" +
                        d.checksum().ToString()) != 0)
      {
        LogCvmfs(kLogCache, kLogSyslog,
                 "failed to move %s to quarantaine", temp_path.c_str());
      }
      result = -EIO;
      goto fetch_finalize;
    }

    LogCvmfs(kLogCache, kLogDebug, "trying to commit %s", final_path.c_str());
    fclose(f);
    fd = -1;
    fd_return = ::open(temp_path.c_str(), O_RDONLY);
    if (fd_return < 0) {
      result = -errno;
      goto fetch_finalize;
    }
    result = cache::CommitTransaction(final_path, temp_path, cvmfs_path,
                                      d.checksum(), d.size());
    if (result == 0) {
      platform_disable_kcache(fd_return);
      result = fd_return;
    } else {
      close(fd_return);
    }
  }

 fetch_finalize:
  // Cleanup
  LogCvmfs(kLogCache, kLogDebug, "finalizing download of %s",
           cvmfs_path.c_str());
  if (result < 0) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog, "failed to fetch %s (hash: %s, "
             "error %d)", cvmfs_path.c_str(), d.checksum().ToString().c_str(),
             tls->download_job.error_code);
  }
  if (fd >= 0) {
    if (f) fclose(f);
    else close(fd);
    AbortTransaction(temp_path);
  }

  // Signal the waiting threads and remove the queue
  pthread_mutex_lock(&lock_queues_download_);
  for (unsigned i = 0, s = tls->other_pipes_waiting.size(); i < s; ++i) {
    int fd_dup = dup(result);
    int retval = write(tls->other_pipes_waiting[i], &fd_dup, sizeof(int));
    assert(retval == sizeof(int));
  }
  tls->other_pipes_waiting.clear();
  queues_download_->erase(d.checksum());
  pthread_mutex_unlock(&lock_queues_download_);

  return result;
}


int64_t GetNumDownloads() {
  return atomic_read64(&num_download_);
}


CatalogManager::CatalogManager(const string &repo_name,
                               const bool ignore_signature)
{
  LogCvmfs(kLogCache, kLogDebug, "constructing cache catalog manager");
  repo_name_ = repo_name;
  ignore_signature_ = ignore_signature;
  offline_mode_ = false;
  atomic_init32(&certificate_hits_);
  atomic_init32(&certificate_misses_);
}


/**
 * Specialized initialization that uses a fixed root hash.
 */
bool CatalogManager::InitFixed(const hash::Any &root_hash) {
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog with root hash %s",
           root_hash.ToString().c_str());
  WriteLock();
  bool attached = MountCatalog(PathString("", 0), root_hash, NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  return attached;

}


catalog::Catalog* CatalogManager::CreateCatalog(const PathString &mountpoint,
  catalog::Catalog *parent_catalog)
{
  mounted_catalogs_[mountpoint] = loaded_catalogs_[mountpoint];
  loaded_catalogs_.erase(mountpoint);
  return new catalog::Catalog(mountpoint, parent_catalog);
}


catalog::LoadError CatalogManager::LoadCatalogCas(const hash::Any &hash,
                                                  const string &cvmfs_path,
                                                  std::string *catalog_path)
{
  int64_t size;
  int retval;
  bool pin_retval;

  // Try from cache
  const string cache_path = *cache_path_ + hash.MakePath(1, 2);
  *catalog_path = cache_path + "T";
  retval = rename(cache_path.c_str(), catalog_path->c_str());
  if (retval == 0) {
    LogCvmfs(kLogCache, kLogDebug, "found catalog %s in cache",
             hash.ToString().c_str());

    size = GetFileSize(catalog_path->c_str());
    assert(size > 0);
    pin_retval = quota::Pin(hash, uint64_t(size), cvmfs_path);
    if (!pin_retval) {
      quota::Remove(hash);
      unlink(catalog_path->c_str());
      LogCvmfs(kLogCache, kLogDebug,
               "failed to pin cached copy of catalog %s",
               hash.ToString().c_str());
      return catalog::kLoadNoSpace;
    }
    // Pinned, can be safely renamed
    retval = rename(catalog_path->c_str(), cache_path.c_str());
    *catalog_path = cache_path;
    return catalog::kLoadNew;
  }

  // Download
  string temp_path;
  int catalog_fd = StartTransaction(hash, catalog_path, &temp_path);
  if (catalog_fd < 0)
    return catalog::kLoadFail;

  FILE *catalog_file = fdopen(catalog_fd, "w");
  if (!catalog_file) {
    AbortTransaction(temp_path);
    return catalog::kLoadFail;
  }

  const string url = "/data" + hash.MakePath(1, 2) + "C";
  download::JobInfo download_catalog(&url, true, true, catalog_file, &hash);
  download::Fetch(&download_catalog);
  fclose(catalog_file);
  if (download_catalog.error_code != download::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "unable to load catalog with key %s (%d)",
             hash.ToString().c_str(), download_catalog.error_code);
    AbortTransaction(temp_path);
    return catalog::kLoadFail;
  }

  size = GetFileSize(temp_path.c_str());
  assert(size > 0);
  if (uint64_t(size) > quota::GetMaxFileSize()) {
    AbortTransaction(temp_path);
    return catalog::kLoadNoSpace;
  }

  // Instead of commit, manually rename and pin, otherwise there is a race
  pin_retval = quota::Pin(hash, uint64_t(size), cvmfs_path);
  if (!pin_retval) {
    AbortTransaction(temp_path);
    return catalog::kLoadNoSpace;
  }

  retval = rename(temp_path.c_str(), catalog_path->c_str());
  if (retval != 0) {
    quota::Remove(hash);
    return catalog::kLoadFail;
  }
  return catalog::kLoadNew;
}


catalog::LoadError CatalogManager::LoadCatalog(const PathString &mountpoint,
                                               const hash::Any &hash,
                                               std::string *catalog_path)
{
  string cvmfs_path = "file catalog at " + repo_name_ + ":" +
    (mountpoint.IsEmpty() ?
      "/" : string(mountpoint.GetChars(), mountpoint.GetLength()));
  bool retval;
  if (!hash.IsNull()) {
    cvmfs_path += " (" + hash.ToString() + ")";
    catalog::LoadError load_error = LoadCatalogCas(hash, cvmfs_path,
                                                   catalog_path);
    if (load_error == catalog::kLoadNew)
      loaded_catalogs_[mountpoint] = hash;
    return load_error;
  }

  // Happens only on init/remount, i.e. quota won't delete a cached catalog

  const string checksum_path = "cvmfschecksum." + repo_name_;
  const string checksum_url = "/.cvmfspublished";
  map<char, string> checksum_keyval;
  size_t checksum_size;
  unsigned char *checksum_buffer;
  hash::Any checksum_hash;
  hash::Any cache_hash;
  hash::Any remote_hash;
  uint64_t cache_last_modified = 0;
  int signature_start = 0;

  // Load local checksum
  FILE *file_checksum = fopen(checksum_path.c_str(), "r");
  char tmp[40];
  if (file_checksum && (fread(tmp, 1, 40, file_checksum) == 40)) {
    cache_hash = hash::Any(hash::kSha1, hash::HexPtr(string(tmp, 40)));
    if (!FileExists("." + cache_hash.MakePath(1, 2))) {
      LogCvmfs(kLogCache, kLogDebug, "found checksum hint without catalog");
      cache_hash = hash::Any();
    } else {
      // Get local last modified time
      char buf_modified;
      string str_modified;
      if ((fread(&buf_modified, 1, 1, file_checksum) == 1) &&
          (buf_modified == 'T'))
      {
        while (fread(&buf_modified, 1, 1, file_checksum) == 1)
          str_modified += string(&buf_modified, 1);
        cache_last_modified = String2Uint64(str_modified);
        LogCvmfs(kLogCache, kLogDebug, "cached copy publish date %s",
                 StringifyTime(cache_last_modified, true).c_str());
      }
    }
  } else {
    LogCvmfs(kLogCache, kLogDebug, "unable to read local checksum");
  }
  if (file_checksum) fclose(file_checksum);

  // Load remote checksum
  download::JobInfo download_checksum(&checksum_url, false, true, NULL);
  download::Fetch(&download_checksum);
  if (download_checksum.error_code != download::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "unable to load checksum from %s (%d)",
             checksum_url.c_str(), download_checksum.error_code);
    if (!cache_hash.IsNull()) {
      // TODO remove code duplication
      if (catalog_path) {
        *catalog_path = "." + cache_hash.MakePath(1, 2);
        int64_t size = GetFileSize(*catalog_path);
        assert(size >= 0);
        retval = quota::Pin(cache_hash, uint64_t(size),
                            cvmfs_path);
        if (!retval) {
          LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
                   "failed to pin cached root catalog");
          return catalog::kLoadFail;
        }
        loaded_catalogs_[mountpoint] = cache_hash;
        offline_mode_ = true;
        return catalog::kLoadUp2Date;
      }
    }
    return catalog::kLoadFail;
  }
  offline_mode_ = false;
  checksum_size = download_checksum.destination_mem.size;

  checksum_buffer = reinterpret_cast<unsigned char *>(alloca(checksum_size));
  memcpy(checksum_buffer, download_checksum.destination_mem.data,
         checksum_size);
  free(download_checksum.destination_mem.data);

  // Parse remote checksum
  ParseKeyvalMem(checksum_buffer, checksum_size,
                 &signature_start, &checksum_hash, &checksum_keyval);

  map<char, string>::const_iterator key_catalog = checksum_keyval.find('C');
  if (key_catalog == checksum_keyval.end()) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "failed to find catalog key in checksum");
    return catalog::kLoadFail;
  }
  remote_hash = hash::Any(hash::kSha1, hash::HexPtr(key_catalog->second));
  cvmfs_path += " (" + remote_hash.ToString() + ")";
  LogCvmfs(kLogCache, kLogDebug, "remote checksum is %s",
           remote_hash.ToString().c_str());

  // Short way out, use cached copy
  if (remote_hash == cache_hash) {
    if (catalog_path) {
      *catalog_path = "." + cache_hash.MakePath(1, 2);
      // quota::Pin is only effective on first load, afterwards it is a NOP
      int64_t size = GetFileSize(*catalog_path);
      assert(size >= 0);
      retval = quota::Pin(cache_hash, uint64_t(size),
                          cvmfs_path);
      if (!retval) {
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
                 "failed to pin cached root catalog");
        return catalog::kLoadFail;
      }
      loaded_catalogs_[mountpoint] = cache_hash;
      return catalog::kLoadUp2Date;
    } else {
      loaded_catalogs_[mountpoint] = cache_hash;
      return catalog::kLoadUp2Date;
    }
  }
  if (!catalog_path)
    return catalog::kLoadNew;

  // Sanity check, last modified (if available, i.e. if signed)
  map<char, string>::const_iterator key_published = checksum_keyval.find('T');
  if (key_published != checksum_keyval.end()) {
    if (cache_last_modified > String2Uint64(key_published->second)) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "cached copy of %s newer than remote copy",
               checksum_url.c_str());
      return catalog::kLoadFail;
    }
  }

  // Sanity check: repository name
  if (repo_name_ != "") {
    map<char, string>::const_iterator key_name = checksum_keyval.find('N');
    if (key_name == checksum_keyval.end()) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "failed to find repository name in checksum");
      return catalog::kLoadFail;
    }
    if (key_name->second != repo_name_) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "expected repository name does not match in %s",
               checksum_url.c_str());
      return catalog::kLoadFail;
    }
  }

  // Sanity check: empty root prefix
  map<char, string>::const_iterator key_root_prefix = checksum_keyval.find('R');
  if (key_root_prefix == checksum_keyval.end()) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "failed to find root prefix in checksum");
    return catalog::kLoadFail;
  }
  if (key_root_prefix->second != hash::Md5(hash::AsciiPtr("")).ToString()) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "expected mount point does not match in %s",
             checksum_url.c_str());
    return catalog::kLoadFail;
  }

  // Verify signature of remote checksum signature
  if (signature_start > 0) {
    // Download certificate
    map<char, string>::const_iterator key_cert = checksum_keyval.find('X');
    if ((key_cert == checksum_keyval.end()) || (key_cert->second.length() < 40))
    {
      LogCvmfs(kLogCache, kLogDebug, "invalid certificate in checksum");
      return catalog::kLoadFail;
    }
    hash::Any cert_hash(hash::kSha1,
                        hash::HexPtr(key_cert->second.substr(0, 40)));

    unsigned char *cert_data;
    uint64_t cert_size;
    if (Open2Mem(cert_hash, &cert_data, &cert_size)) {
      atomic_inc32(&certificate_hits_);
    } else {
      atomic_inc32(&certificate_misses_);

      const string cert_url = "/data" + cert_hash.MakePath(1, 2) + "X";
      download::JobInfo download_certificate(&cert_url, true, true, &cert_hash);
      download::Fetch(&download_certificate);
      if (download_certificate.error_code != download::kFailOk) {
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
                 "unable to load certificate from %s (%d)",
                 cert_url.c_str(), download_certificate.error_code);
        return catalog::kLoadFail;
      }

      cert_data = (unsigned char *)download_certificate.destination_mem.data;
      cert_size = download_certificate.destination_mem.size;
      CommitFromMem(cert_hash, cert_data, cert_size,
                    "certificate of " + signature::Whois());
    }

    // Load certificate
    retval = signature::LoadCertificateMem(cert_data, cert_size);
    free(cert_data);
    if (!retval) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog, "could not read certificate");
      return catalog::kLoadFail;
    }

    // Verify certificate against whitelist
    const string whitelist_url = "/.cvmfswhitelist";
    download::JobInfo download_whitelist(&whitelist_url, false, true, NULL);
    download::Fetch(&download_whitelist);
    if (download_whitelist.error_code != download::kFailOk) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "unable to load whitelist from %s (%d)",
               whitelist_url.c_str(), download_whitelist.error_code);
      return catalog::kLoadFail;
    }
    retval = signature::VerifyWhitelist(download_whitelist.destination_mem.data,
                                        download_whitelist.destination_mem.size,
                                        repo_name_);
    free(download_whitelist.destination_mem.data);
    if (!retval) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
               "whitelist verification failed");
      return catalog::kLoadFail;
    }

    // Verify checksum signature
    unsigned char *signature_buffer;
    unsigned signature_size;
    retval = signature::ReadSignatureTail(checksum_buffer, checksum_size,
                                          signature_start,
                                          &signature_buffer, &signature_size);
    if (!retval) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog, "cannot read signature");
      return catalog::kLoadFail;
    }
    retval = signature::Verify(
      reinterpret_cast<const unsigned char *>(&((checksum_hash.ToString())[0])),
      40, signature_buffer, signature_size);
    free(signature_buffer);
    if (!retval) {
      LogCvmfs(kLogCache, kLogDebug,
               "catalog signature verification failed against %s",
               checksum_hash.ToString().c_str());
      return catalog::kLoadFail;
    }
    LogCvmfs(kLogCache, kLogSyslog,
             "catalog signature verification passed, signed by %s",
             signature::Whois().c_str());
  } else {
    LogCvmfs(kLogCache, kLogDebug, "remote checksum is not signed");
    if (!ignore_signature_) {
      LogCvmfs(kLogCache, kLogSyslog, "remote checksum %s is not signed",
               checksum_url.c_str());
      return catalog::kLoadFail;
    }
  }

  catalog::LoadError load_retval = LoadCatalogCas(remote_hash, cvmfs_path,
                                                  catalog_path);
  if (load_retval != catalog::kLoadNew)
    return load_retval;

  loaded_catalogs_[mountpoint] = remote_hash;

  // Store checksum
  int fdchksum = open(checksum_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fdchksum >= 0) {
    string cache_checksum = remote_hash.ToString();
    map<char, string>::const_iterator key_published = checksum_keyval.find('T');
    if (key_published != checksum_keyval.end())
      cache_checksum += "T" + key_published->second;

    file_checksum = fdopen(fdchksum, "w");
    if (file_checksum) {
      if (fwrite(&(cache_checksum[0]), 1, cache_checksum.length(),
                 file_checksum) != cache_checksum.length())
      {
        unlink(checksum_path.c_str());
      }
      fclose(file_checksum);
    } else {
      unlink(checksum_path.c_str());
    }
  } else {
    unlink(checksum_path.c_str());
  }

  return catalog::kLoadNew;
}


void CatalogManager::UnloadCatalog(const catalog::Catalog *catalog) {
  LogCvmfs(kLogCache, kLogDebug, "unloading catalog %s", 
           catalog->path().c_str());

  map<PathString, hash::Any>::iterator iter =
    mounted_catalogs_.find(catalog->path());
  assert(iter != mounted_catalogs_.end());

  quota::Unpin(iter->second);
  mounted_catalogs_.erase(iter);
}

}  // namespace cache
