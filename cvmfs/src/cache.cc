/**
 * \file cache.cc
 * \namespace cache
 *
 * The cache module maintains the local file cache.  Files are
 * requested by fetch().  The cache stores files with a name according
 * to their SHA1-hash.
 *
 * The procedure is
 *   -# Look in the catalog for SHA1 hash
 *   -# If it is in local cache: return file descriptor
 *   -# Otherwise download, store in cache and return fd
 *
 * Each running CVMFS instance has to have a separate cache directory.
 * The local cache directory (directories 00..ff) can be accessed
 * in parallel to a running CVMFS, i.e. files can be deleted for instance
 * anytime.  Files are created in txn directory first.  At the very latest
 * point they are renamed into their "real" SHA1 names atomically by rename().
 * This concept ist taken over from GROW-FS.
 *
 * Download of files is protected by a mutex passed by cvmfs.cc.
 * This avoids wasting the network channel with downloading
 * the same file multiple times.
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "cache.h"

#include "catalog.h"
#include "DirectoryEntry.h"
#include "lru.h"
#include "util.h"
#include "hash.h"

#include "compat.h"

#include <string>
#include <sstream>
#include <queue>

#include <cassert>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>

#include "logging.h"
extern "C" {
   #include "http_curl.h"
   #include "debug.h"
   #include "sha1.h"
   #include "smalloc.h"
   #include "compression.h"
}

using namespace std;

namespace cache {

   /* These will be set on init */
   string cache_path = "";
   string root_url = "";
   pthread_mutex_t *mutex_download = NULL;

   /**
    * Inits cache directory.
    * We get some global parameters and options from cvmfs.cc here.
    *
    * \return True on success, false otherwise
    */
   bool init(const string &c_path, const string &r_url, pthread_mutex_t * const m_download)
   {
      cache_path = c_path;
      root_url = r_url;
      mutex_download = m_download;

      if (!make_cache_dir(cache_path, 0700))
         return false;

      /* Cleanup dangling checksums */
      DIR *dirp = NULL;
      dirent *d;
      if ((dirp = opendir(cache_path.c_str())) == NULL) {
         pmesg(D_CACHE, "failed to open directory %s", cache_path.c_str());
         return false;
      }
      while ((d = readdir(dirp)) != NULL) {
         if (d->d_type != DT_REG) continue;

         const string name = d->d_name;
         if (name.substr(0, 14) == "cvmfs.checksum") {
            const string current_path = cache_path + "/" + name;
            FILE *f = fopen(current_path.c_str(), "r");
            if (f != NULL) {
               char sha1[40];
               if (fread(sha1, 1, 40, f) == 40) {
                  const string sha1_str = string(sha1, 40);
                  pmesg(D_CACHE, "found checksum %s", sha1_str.c_str());
                  const string sha1_path = cache_path + "/" + sha1_str.substr(0,2) +
                                           "/" + sha1_str.substr(2);

                  /* legacy handling */
                  unlink((cache_path + "/" + "cvmfs.catalog" + name.substr(14)).c_str());

                  if (!file_exists(sha1_path))
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

      return true;
   }


   /**
    * NOP currently.
    */
   void fini() {
   }


   /**
    * Transforms a catalog entry into a name for local cache.
    *
    * @param[in] id SHA1 checksum of the catalog entry.
    * \return Absolute path in local cache.
    */
   static string cached_name(const hash::t_sha1 &id) {
      const string hash_path = id.to_string();
      return cache_path + "/" + hash_path.substr(0, 2) + "/" + hash_path.substr(2);
   }


   /**
    * Transform a catalog entry into a temporary name in txn-directory.
    *
    * @param[in] id SHA1 checksum of the catalog entry.
    * \return Absolute path in local cache txn-directory.
    */
   static string txn_name(const hash::t_sha1 &id)
   {
      const string hash_path = id.to_string();
      return cache_path + "/txn/" + hash_path.substr(6) + "XXXXXX";
   }


   /**
    * Tries to open a catalog entry in local cache.
    *
    * @param[in] id SHA1 checksum of the catalog entry.
    * \return A file descriptor if file is in cache.  Error code of open() else.
    */
   int open(const hash::t_sha1 &id) {
      const string lpath = cached_name(id);
      int result = ::open(lpath.c_str(), O_RDONLY, 0);

      if (result >= 0) pmesg(D_CACHE, "hit %s", lpath.c_str());
      else pmesg(D_CACHE, "miss %s (%d)", lpath.c_str(), result);

      return result;
   }


   /**
    * Starts a "transaction" based on a catalog entry, i.e. start download.
    *
    * @param[in] id SHA1 checksum of the catalog entry.
    * @param[out] lpath Absolute path of the file in local cache after commit
    * @param[out] txn Absolute path of the temporoary file in local cache
    * \return File descriptor of temporary file, error code of mkstemp() else
    */
   int transaction(const hash::t_sha1 &id, string &lpath, string &txn)
   {
      int result;
      lpath = cached_name(id);
      txn = txn_name(id);
      char * const templ = (char *)alloca(txn.length() + 1);
      strncpy(templ, txn.c_str(), txn.length() + 1);
      result = ::mkstemp(templ);
      txn = templ;
      if (result >= 0) {
         pmesg(D_CACHE, "begin %s", txn.c_str());
         if (fchmod(result, 0700) != 0) {
            pmesg(D_CACHE, "fchmod failed on %s with %d", txn.c_str(), -errno);
            unlink(txn.c_str());
            close(result);
            result = -1;
         }
      } else {
         pmesg(D_CACHE, "transaction on %s failed with %d", txn.c_str(), -errno);
      }
      return result;
   }


   /**
    * Aborts a file download started with transaction() and cleans
    * temporoary storage.
    *
    * @param[in] txn Absolute path of the temporoary file in local cache
    * \return Zero on success, error code of unlink() else.
    */
   int abort(const string &txn) {
      pmesg(D_CACHE,"abort %s", txn.c_str());
      return unlink(txn.c_str());
   }


   /**
    * Commits a file download started with transaction(), i.e. renames
    * the txn-file to its real SHA1 name.
    *
    * If the cache is managed (quota / lru), it also inserts the file into
    * the lru database.
    *
    * @param[in] txn Absolute path of the temporoary file in local cache
    * @param[in] lpath Absolute SHA1 path of the file in local cache
    * \return Zero on success, non-zero else.
    */
   int commit(const string &lpath, const string &txn, const string &cvmfs_path,
              const hash::t_sha1 &sha1, const uint64_t size)
   {
      int result;
      pmesg(D_CACHE, "commit %s %s", lpath.c_str(), txn.c_str());
      result = rename(txn.c_str(), lpath.c_str());
      if (result < 0) {
         pmesg(D_CACHE, "commit failed: %s", strerror(errno));
         unlink(txn.c_str());
      } else {
         if (!lru::insert(sha1, size, cvmfs_path)) {
            pmesg(D_CACHE, "insert into lru failed");
            unlink(lpath.c_str());
            result = -1;
         }
      }
      return result;
   }


   /**
    * Checks for a file in local cache.  Because we support parallel operations,
    * this is just a hint.  Open gives a definitive answer.
    *
    * @param[in] id SHA1 checksum of the catalog entry.
    * \return True, if file is in local cache, false otherwise.
    */
   bool contains(const hash::t_sha1 &id) {
      struct stat info;
      return stat(cached_name(id).c_str(), &info) == 0;
   }

   /**
    * Tries to find a file in the local disk cache.  If it is not
    * available, it locks download_mutex for the succeeding fetch
    * operation (crowd or http).
    *
    * \return Read-only file descriptor on success, -1 else
    */
   int open_or_lock(const cvmfs::DirectoryEntry &d) {
      int fd;

      if ((fd = cache::open(d.checksum())) >= 0) {
         lru::touch(d.checksum());
         return fd;
      }

      pthread_mutex_lock(mutex_download);
      /* We have to check again to avoid race condition */
      if ((fd = cache::open(d.checksum())) >= 0) {
         pthread_mutex_unlock(mutex_download);
         lru::touch(d.checksum());
         return fd;
      }

      return -1;
   }

   static bool freset(FILE *f) {
      if (fflush(f) != 0)
         return false;
      if (ftruncate(fileno(f), 0) != 0)
         return false;
      rewind(f);
      return true;
   }


   /**
    * Returns a read-only file descriptor for a specific catalog entry.
    * After successful call, the file resides in local cache.
    * File is downloaded via HTTP.
    * This function expects mutex_download to be already acquired.
    *
    * During download, SHA1 checksum is calculated and afterwards compared
    * to catalog.  If it doesn't match, we try again avoiding proxies in order
    * to circumvent outdated proxy caches.
    *
    * @param[in] d Demanded catalog entry
    * @param[in] path Relative path of file on the HTTP server
    * \return Read-only file descriptor for the file pointing into local cache.
    *         On failure a negative error code.
    */
   int fetch(const cvmfs::DirectoryEntry &d, const string &path)
   {
      string url;
      string lpath;
      string txn;
      hash::t_sha1 sha1;
      int fd, fd_return;
      int result = -EIO;
      int retval;
      char strmbuf[4096];

      /* Not in cache, we are holding the mutex and download */
      int curl_result;
      bool nocache = false; /* try once with no-cache-download after failure */
      FILE *f = NULL;

      if (d.size() > lru::max_file_size()) {
         pmesg(D_CACHE, "file too big for lru cache");
         return -ENOSPC;
      }

      pmesg(D_CACHE, "loading %s", path.c_str());

      const string hash_path = d.checksum().to_string();
      url = "/data/" + hash_path.substr(0, 2) + "/" + hash_path.substr(2);
      pmesg(D_CACHE, "curl fetches %s", url.c_str());

      fd = cache::transaction(d.checksum(), lpath, txn);
      if (fd < 0) {
         pmesg(D_CACHE, "could not start transaction on %s", lpath.c_str());
         return -EIO;
      }

      f = fdopen(fd, "r+");
      if (!f) {
         result = -errno;
         pmesg(D_CACHE, "could not fdopen %s", lpath.c_str());
         goto fetch_abort;
      }
      retval = setvbuf(f, strmbuf, _IOFBF, 4096);
      assert(retval == 0);

      curl_result = curl_download_stream(url.c_str(), f, sha1.digest, 1, 1);

   download_retry:
      if ((curl_result == CURLE_OK) || (curl_result == Z_DATA_ERROR)) {
         pmesg(D_CACHE, "curl finished downloading of %s, checksum: %s", url.c_str(), sha1.to_string().c_str());

         /* Check checksum, if doesn't match, skip proxy
            if proxy already skipped, reload catalog
            if catalog is fresh: error */
         if ((d.checksum() != sha1) || (curl_result == Z_DATA_ERROR)) {
            if (!nocache) {
               LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
                        "Checksum does not match for %s (SHA1: %s). "
                        "I'll retry download with no-cache",
                        path.c_str(), d.checksum().to_string().c_str());
               if (!freset(f))
                  goto fetch_abort;
               nocache = true;
               curl_result = curl_download_stream_nocache(url.c_str(), f, sha1.digest, 1, 1);
               goto download_retry;
            }
            pmesg(D_CACHE, "no-cache didn't help, aborting now");
            goto fetch_abort;
         }

         /* Check decompressed size */
         struct stat64 info;
         info.st_size = -1;
         if ((fstat64(fileno(f), &info) != 0) || (info.st_size != (signed)d.size())) {
            LogCvmfs(kLogCache, kLogSyslog,
                     "size check failure for %s, expected %lu, got %ld",
                     url.c_str(), d.size(), info.st_size);
            if (file_copy(txn.c_str(), (cache_path + "/quarantaine/" + d.checksum().to_string()).c_str()) != 0)
               LogCvmfs(kLogCache, kLogSyslog,
                        "failed to move %s to quarantaine", txn.c_str());
            if (!nocache) {
               LogCvmfs(kLogCache, kLogSyslog, "Re-trying %s with no-cache",
                        path.c_str());
               if (!freset(f))
                  goto fetch_abort;
               nocache = true;
               curl_result = curl_download_stream_nocache(url.c_str(), f, sha1.digest, 1, 1);
               goto download_retry;
            }
            goto fetch_abort;
         }

         pmesg(D_CACHE, "trying to commit");
         fclose(f);
         fd_return = ::open(txn.c_str(), O_RDONLY, 0);
         if (fd_return < 0) {
            result = -errno;
            return result;
         }
         if (cache::commit(lpath, txn, path, d.checksum(), d.size()) == 0) {
            return fd_return;
         } else {
            close(fd_return);
            return -EIO;
         }
      }

   fetch_abort:
      LogCvmfs(kLogCache, kLogSyslog, "failed to fetch %s (SHA1: %s)",
               path.c_str(), d.checksum().to_string().c_str());
      if (fd >= 0) {
         if (f) fclose(f);
         else close(fd);
         abort(txn);
      }
      return result;
   }

   bool mem_to_disk(const hash::t_sha1 &id, const char *buffer, const size_t size,
                    const std::string &name)
   {
      string txn;
      string path;

      int fd = transaction(id, path, txn);
      if (fd < 0)
         return false;

      ssize_t retval = write(fd, buffer, size);
      close(fd);
      if ((retval < 0) || ((size_t)retval != size)) {
         abort(txn);
         return false;
      }

      return commit(path, txn, name, id, size) == 0;
   }

   bool disk_to_mem(const hash::t_sha1 &id, char **buffer, size_t *size) {
      *size = 0;
      *buffer = NULL;

      int fd = open(id);

      if (fd < 0)
         return false;

      PortableStat64 info;
      if (portableFileDescriptorStat64(fd, &info) != 0) {
         close(fd);
         return false;
      }

      *size = info.st_size;
      *buffer = (char *)smalloc(*size);

      ssize_t retval = read(fd, *buffer, *size);
      if ((retval < 0) || ((size_t)retval != *size)) {
         close(fd);
         free(*buffer);
         *buffer = NULL;
         *size = 0;
         return false;
      }

      close(fd);
      return true;
   }

}
