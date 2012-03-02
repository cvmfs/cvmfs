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

#include "cvmfs_config.h"
#include "cache.h"

#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>

#include <cassert>
#include <cstring>
#include <cstdlib>
#include <cstdio>

#include <map>
#include <vector>

#include "platform.h"
#include "DirectoryEntry.h"
#include "lru.h"
#include "util.h"
#include "hash.h"
#include "logging.h"
#include "download.h"
#include "compression.h"
#include "smalloc.h"

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
string *root_url_ = NULL;
ThreadQueues *queues_download_ = NULL;  /**< maps currently
  downloaded chunks to an array of writer's ends of a pipe to signal the waiting
  threads when the download has finished */
pthread_mutex_t lock_queues_download_ = PTHREAD_MUTEX_INITIALIZER;
pthread_key_t thread_local_storage_;


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
bool Init(const string &cache_path, const string &root_url) {
  cache_path_ = new string(cache_path);
  root_url_ = new string(root_url);
  queues_download_ = new ThreadQueues();

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
  delete root_url_;
  delete queues_download_;
  cache_path_ = NULL;
  root_url_ = NULL;
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
    posix_fadvise(result, 0, 0, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);
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
bool Open2Mem(const hash::Any &id, char **buffer, uint64_t *size) {
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
  *buffer = static_cast<char *>(smalloc(*size));

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
    if (!lru::Insert(hash, size, cvmfs_path)) {
      LogCvmfs(kLogCache, kLogDebug, "insert into lru failed");
      unlink(final_path.c_str());
      result = -1;
    }
  }

  return result;
}


/**
 * Commits the memory blob buffer to the given chunk id and name on cvmfs.
 * No checking! The hash and the memory blob need to match.
 */
bool CommitFromMem(const hash::Any &id, const char *buffer,
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
int Fetch(const cvmfs::DirectoryEntry &d, const string &cvmfs_path)
{
  int fd_return;  // Read-only file descriptor that is returned
  int retval;

  if (d.size() > lru::GetMaxFileSize()) {
    LogCvmfs(kLogCache, kLogDebug, "file too big for lru cache");
    return -ENOSPC;
  }

  // Try to open from local cache
  if ((fd_return = cache::Open(d.checksum_)) >= 0) {
    lru::Touch(d.checksum_);
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
  ThreadQueues::iterator iDownloadQueue = queues_download_->find(d.checksum_);
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
    fd_return = cache::Open(d.checksum_);
    if (fd_return >= 0) {
      pthread_mutex_unlock(&lock_queues_download_);
      lru::Touch(d.checksum_);
      return fd_return;
    }

    // Create a new queue for this chunk
    (*queues_download_)[d.checksum_] = &tls->other_pipes_waiting;
    pthread_mutex_unlock(&lock_queues_download_);
  }

  // The download path starts here
  LogCvmfs(kLogCache, kLogDebug, "downloading %s", cvmfs_path.c_str());

  const string url = "/data" + d.checksum_.MakePath(1, 2);
  string final_path;
  string temp_path;
  int fd;  // Used to write the downloaded file
  FILE *f = NULL;
  char strmbuf[4096];  // Place FILE stream buffer on the stack
  // TODO(jakob): Reuse in thread
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
  retval = setvbuf(f, strmbuf, _IOFBF, 4096);
  assert(retval == 0);

  tls->download_job.url = &url;
  tls->download_job.destination_file = f;
  tls->download_job.expected_hash = &d.checksum_;
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
                                      d.checksum_, d.size());
    if (result == 0) {
      posix_fadvise(fd_return, 0, 0, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);
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
             "error %d)", cvmfs_path.c_str(), d.checksum_.ToString().c_str(),
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
  queues_download_->erase(d.checksum_);
  pthread_mutex_unlock(&lock_queues_download_);

  return result;
}

}  // namespace cache
