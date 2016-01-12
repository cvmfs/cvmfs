/**
 * This file is part of the CernVM File System.
 *
 * This tool checks a cvmfs cache directory for consistency.
 * If necessary, the managed cache db is removed so that
 * it will be rebuilt on next mount.
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "atomic.h"
#include "compression.h"
#include "hash.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

enum Errors {
  kErrorOk = 0,
  kErrorFixed = 1,
  kErrorReboot = 2,
  kErrorUnfixed = 4,
  kErrorOperational = 8,
  kErrorUsage = 16,
};

string *g_cache_dir;
atomic_int32 g_num_files;
atomic_int32 g_num_err_fixed;
atomic_int32 g_num_err_unfixed;
atomic_int32 g_num_err_operational;
atomic_int32 g_num_tmp_catalog;
/**
 * Traversal of the file system tree is serialized.
 */
pthread_mutex_t g_lock_traverse = PTHREAD_MUTEX_INITIALIZER;
DIR *g_DIRP_current = NULL;
int g_num_dirs = -1;  /**< Number of cache directories already examined. */
string *g_current_dir;  /**< Current cache sub directory */

int g_num_threads = 1;
bool g_fix_errors = false;
bool g_verbose = false;
atomic_int32 g_force_rebuild;
atomic_int32 g_modified_cache;


static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "CernVM File System consistency checker, version %s\n\n"
           "This tool checks a cvmfs cache directory for consistency.\n"
           "If necessary, the managed cache db is removed so that\n"
           "it will be rebuilt on next mount.\n\n"
           "Usage: cvmfs_fsck [-v] [-p] [-f] [-j #threads] <cache directory>\n"
           "Options:\n"
           "  -v verbose output\n"
           "  -p try to fix automatically\n"
           "  -f force rebuild of managed cache db on next mount\n"
           "  -j number of concurrent integrity check worker threads\n",
           VERSION);
}


static bool GetNextFile(string *relative_path, string *hash_name) {
  platform_dirent64 *d = NULL;

  pthread_mutex_lock(&g_lock_traverse);
 get_next_file_again:
  while (g_DIRP_current && ((d = platform_readdir(g_DIRP_current)) != NULL)) {
    const string name = d->d_name;
    if ((name == ".") || (name == "..")) continue;

    platform_stat64 info;
    *relative_path = *g_current_dir + "/" + name;
    *hash_name = *g_current_dir + name;
    const string path = *g_cache_dir + "/" + *relative_path;
    if (platform_lstat(relative_path->c_str(), &info) != 0) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Warning: failed to stat() %s (%d)",
               path.c_str(), errno);
      continue;
    }

    if (!S_ISREG(info.st_mode)) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Warning: %s is not a regular file",
               path.c_str());
      continue;
    }

    break;
  }

  if (!d) {
    if (g_DIRP_current) {
      closedir(g_DIRP_current);
      g_DIRP_current = NULL;
    }
    g_num_dirs++;
    if (g_num_dirs < 256) {
      char hex[3];
      snprintf(hex, sizeof(hex), "%02x", g_num_dirs);
      *g_current_dir = string(hex, 2);

      if (g_verbose)
        LogCvmfs(kLogCvmfs, kLogStdout, "Entering %s", g_current_dir->c_str());
      if ((g_DIRP_current = opendir(hex)) == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Invalid cache directory, %s/%s does not exist",
                 g_cache_dir->c_str(), g_current_dir->c_str());
        pthread_mutex_unlock(&g_lock_traverse);
        exit(kErrorUnfixed);
      }
      goto get_next_file_again;
    }
  }
  pthread_mutex_unlock(&g_lock_traverse);

  if (d)
    return true;

  return false;
}


static void *MainCheck(void *data __attribute__((unused))) {
  string relative_path;
  string hash_name;

  while (GetNextFile(&relative_path, &hash_name)) {
    const string path = *g_cache_dir + "/" + relative_path;

    int n = atomic_xadd32(&g_num_files, 1);
    if (g_verbose)
      LogCvmfs(kLogCvmfs, kLogStdout, "Checking file %s", path.c_str());
    if (!g_verbose && ((n % 1000) == 0))
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, ".");

    if (relative_path[relative_path.length()-1] == 'T') {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "Warning: temporary file catalog %s found", path.c_str());
      atomic_inc32(&g_num_tmp_catalog);
      if (g_fix_errors) {
        if (unlink(relative_path.c_str()) == 0) {
          LogCvmfs(kLogCvmfs, kLogStdout, "Fix: %s unlinked", path.c_str());
          atomic_inc32(&g_num_err_fixed);
        } else {
          LogCvmfs(kLogCvmfs, kLogStdout, "Error: failed to unlink %s",
                   path.c_str());
          atomic_inc32(&g_num_err_unfixed);
        }
      }
      continue;
    }

    int fd_src = open(relative_path.c_str() , O_RDONLY);
    if (fd_src < 0) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Error: cannot open %s", path.c_str());
      atomic_inc32(&g_num_err_operational);
      continue;
    }
    // Don't thrash kernel buffers
    platform_disable_kcache(fd_src);

    // Compress every file and calculate SHA-1 of stream
    shash::Any expected_hash = shash::MkFromHexPtr(shash::HexPtr(hash_name));
    shash::Any hash(expected_hash.algorithm);
    if (!zlib::CompressFd2Null(fd_src, &hash)) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Error: could not compress %s",
               path.c_str());
      atomic_inc32(&g_num_err_operational);
    } else {
      if (hash != expected_hash) {
        // If the hashes don't match, try hashing the uncompressed file
        if (!shash::HashFile(relative_path, &hash)) {
          LogCvmfs(kLogCvmfs, kLogStdout, "Error: could not hash %s",
                   path.c_str());
          atomic_inc32(&g_num_err_operational);
        }
        if (hash != expected_hash) {
          if (g_fix_errors) {
            const string quarantaine_path = "./quarantaine/" + hash_name;
            bool fixed = false;
            if (rename(relative_path.c_str(), quarantaine_path.c_str()) == 0) {
              LogCvmfs(kLogCvmfs, kLogStdout,
                       "Fix: %s is corrupted, moved to quarantaine folder",
                       path.c_str());
              fixed = true;
            } else {
              LogCvmfs(kLogCvmfs, kLogStdout,
                       "Warning: failed to move %s into quarantaine folder",
                       path.c_str());
              if (unlink(relative_path.c_str()) == 0) {
                LogCvmfs(kLogCvmfs, kLogStdout,
                         "Fix: %s is corrupted, file unlinked", path.c_str());
                fixed = true;
              } else {
                LogCvmfs(kLogCvmfs, kLogStdout,
                         "Error: %s is corrupted, could not unlink",
                         path.c_str());
              }
            }

            if (fixed) {
              atomic_inc32(&g_num_err_fixed);

              // Changes made, we have to rebuild the managed cache db
              atomic_cas32(&g_force_rebuild, 0, 1);
              atomic_cas32(&g_modified_cache, 0, 1);
            } else {
              atomic_inc32(&g_num_err_unfixed);
            }
          } else {
            LogCvmfs(kLogCvmfs, kLogStdout,
                     "Error: %s has compressed checksum %s, "
                     "delete this file from cache directory!",
                     path.c_str(), hash.ToString().c_str());
            atomic_inc32(&g_num_err_unfixed);
          }
        }
      }
    }
    close(fd_src);
  }

  return NULL;
}


int main(int argc, char **argv) {
  atomic_init32(&g_force_rebuild);
  atomic_init32(&g_modified_cache);
  g_current_dir = new string();

  int c;
  while ((c = getopt(argc, argv, "hvpfj:")) != -1) {
    switch (c) {
      case 'h':
        Usage();
        return kErrorOk;
      case 'v':
        g_verbose = true;
        break;
      case 'p':
        g_fix_errors = true;
        break;
      case 'f':
        atomic_cas32(&g_force_rebuild, 0, 1);
        break;
      case 'j':
        g_num_threads = atoi(optarg);
        if (g_num_threads < 1) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "There is at least one worker thread required");
          return kErrorUsage;
        }
        break;
      case '?':
      default:
        Usage();
        return kErrorUsage;
    }
  }

  // Switch to cache directory
  if (optind >= argc) {
    Usage();
    return kErrorUsage;
  }
  g_cache_dir = new string(MakeCanonicalPath(argv[optind]));
  if (chdir(g_cache_dir->c_str()) != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not chdir to %s",
             g_cache_dir->c_str());
    return kErrorOperational;
  }

  // Check if txn directory is empty
  DIR *dirp_txn;
  if ((dirp_txn = opendir("txn")) == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Invalid cache directory, %s/txn does not exist",
             g_cache_dir->c_str());
    return kErrorOperational;
  }
  platform_dirent64 *d;
  while ((d = platform_readdir(dirp_txn)) != NULL) {
    const string name = d->d_name;
    if ((name == ".") || (name == "..")) continue;

    LogCvmfs(kLogCvmfs, kLogStdout,
             "Warning: temporary directory %s/txn is not empty\n"
             "If this repository is currently _not_ mounted, "
             "you can remove its contents", g_cache_dir->c_str());
    break;
  }
  closedir(dirp_txn);

  // Run workers to recalculate checksums
  atomic_init32(&g_num_files);
  atomic_init32(&g_num_err_fixed);
  atomic_init32(&g_num_err_unfixed);
  atomic_init32(&g_num_err_operational);
  atomic_init32(&g_num_tmp_catalog);
  pthread_t *workers = reinterpret_cast<pthread_t *>(
    smalloc(g_num_threads * sizeof(pthread_t)));
  if (!g_verbose)
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "Verifying: ");
  for (int i = 0; i < g_num_threads; ++i) {
    if (g_verbose)
      LogCvmfs(kLogCvmfs, kLogStdout, "Starting worker %d", i+1);
    if (pthread_create(&workers[i], NULL, MainCheck, NULL) != 0) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Fatal: could not create worker thread");
      return kErrorOperational;
    }
  }
  for (int i = g_num_threads-1; i >= 0; --i) {
    pthread_join(workers[i], NULL);
    if (g_verbose)
      LogCvmfs(kLogCvmfs, kLogStdout, "Stopping worker %d", i+1);
  }
  free(workers);
  if (!g_verbose)
    LogCvmfs(kLogCvmfs, kLogStdout, "");
  LogCvmfs(kLogCvmfs, kLogStdout, "Verified %d files",
           atomic_read32(&g_num_files));

  if (atomic_read32(&g_num_tmp_catalog) > 0)
    LogCvmfs(kLogCvmfs, kLogStdout, "Temporary file catalogs were found.");

  if (atomic_read32(&g_force_rebuild)) {
    if (unlink("cachedb") == 0) {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "Fix: managed cache db unlinked, will be rebuilt on next mount");
      atomic_inc32(&g_num_err_fixed);
    } else {
      if (errno != ENOENT) {
        LogCvmfs(kLogCvmfs, kLogStdout,
                 "Error: could not unlink managed cache database (%d)", errno);
        atomic_inc32(&g_num_err_unfixed);
      }
    }
  }

  if (atomic_read32(&g_modified_cache)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "\n"
             "WARNING: There might by corrupted files in the kernel buffers.\n"
             "Remount CernVM-FS or run 'echo 3 > /proc/sys/vm/drop_caches'"
             "\n\n");
  }

  int retval = 0;
  if (atomic_read32(&g_num_err_fixed) > 0)
    retval |= kErrorFixed;
  if (atomic_read32(&g_num_err_unfixed) > 0)
    retval |= kErrorUnfixed;
  if (atomic_read32(&g_num_err_operational) > 0)
    retval |= kErrorOperational;

  return retval;
}
