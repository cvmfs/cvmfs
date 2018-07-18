/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include <pthread.h>
#include <stdio.h>
#include <fstream>

#include "atomic.h"
#include "fs_traversal.h"
#include "fs_traversal_interface.h"
#include "fs_traversal_libcvmfs.h"
#include "fs_traversal_posix.h"
#include "libcvmfs.h"
#include "logging.h"
#include "smalloc.h"
#include "util/posix.h"
#include "util/string.h"

#define COPY_BUFFER_SIZE 4096

using namespace std; //NOLINT

// Taken from fsck
enum Errors {
  kErrorOk = 0,
  kErrorFixed = 1,
  kErrorReboot = 2,
  kErrorUnfixed = 4,
  kErrorOperational = 8,
  kErrorUsage = 16,
};

int strcmp_list(const void* a, const void* b)
{
    char const **char_a = (char const **)a;
    char const **char_b = (char const **)b;

    return strcmp(*char_a, *char_b);
}

namespace shrinkwrap {

namespace {

class FileCopy {
 public:
  FileCopy() {}

  FileCopy(const char *src, const char *dest)
    : src(src)
    , dest(dest) {}

  bool IsTerminateJob() const {
    return ((src == NULL) && (dest == NULL));
  }

  const char *src;
  const char *dest;
};

unsigned             num_parallel;
bool                 recursive;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
unsigned             retries = 1;
atomic_int64         overall_copies;
atomic_int64         overall_new;
atomic_int64         copy_queue;

}  // namespace

struct fs_traversal* FindInterface(const char * type)
{
  if (!strcmp(type, "posix")) {
    return posix_get_interface();
  } else if (!strcmp(type, "cvmfs")) {
    return libcvmfs_get_interface();
  }
  LogCvmfs(kLogCvmfs, kLogStderr,
  "Unknown File System Interface : %s", type);
  return NULL;
}

bool copy_file(
  struct fs_traversal *src_fs,
  const char *src_data,
  struct fs_traversal *dest_fs,
  const char *dest_data,
  int parallel);

bool cvmfs_attr_cmp(struct cvmfs_attr *src, struct cvmfs_attr *dest,
    struct fs_traversal *dest_fs) {
  if (!src) { return false; }
  if (!dest) { return false; }
  if (src->version != dest->version) { return false; }
  if (src->size != dest->size) { return false; }

  // Actual contents of stat, mapped from DirectoryEntry
  if (src->st_ino != dest->st_ino)     { return false; }
  if (src->st_mode != dest->st_mode)   { return false; }
  if (src->st_nlink != dest->st_nlink) { return false; }
  if (src->st_uid != dest->st_uid)     { return false; }
  if (src->st_gid != dest->st_gid)     { return false; }
  if (src->st_rdev != dest->st_rdev)   { return false; }
  if (src->st_size != dest->st_size)   { return false; }
  if (src->mtime != dest->mtime)       { return false; }

/*
  // CVMFS related content
  if (src->cvm_checksum) {
    if (!dest_fs->is_hash_consistent(dest_fs->context_, src)) { return false; }
  } else if (!src->cvm_checksum && dest->cvm_checksum) {
    return false;
  }
*/
  if (src->cvm_symlink && dest->cvm_symlink) {
    if (strcmp(src->cvm_symlink , dest->cvm_symlink)) { return false; }
  } else if ( (!src->cvm_symlink && dest->cvm_symlink) ||
              (src->cvm_symlink && !dest->cvm_symlink) ) {
    return false;
  }

  if (src->cvm_name && dest->cvm_name) {
    if (strcmp(src->cvm_name, dest->cvm_name)) { return false; }
  } else if ((!src->cvm_name && dest->cvm_name) ||
            (src->cvm_name && !dest->cvm_name)) {
    return false;
  }

  // TODO(nhazekam): Not comparing Xattrs yet
  // void *       cvm_xattrs;
  return true;
}

char *get_full_path(const char *dir, const char *entry) {
  size_t len = 2 + strlen(dir)+ strlen(entry);
  char * path = reinterpret_cast<char *>(malloc(len));
  snprintf(path, len, "%s/%s",  dir, entry);
  return path;
}

bool getNext(
  struct fs_traversal *fs,
  const char *dir,
  char **dir_list,
  char **entry,
  size_t *iter
) {
  size_t location = 0;
  if (iter) {
    location = (*iter)+1;
    *iter = location;
  }

  free(*entry);
  *entry = NULL;

  if (dir_list[location]) {
    *entry = get_full_path(dir, dir_list[location]);
  } else {
    return false;
  }
  return true;
}

bool updateStat(
  struct fs_traversal *fs,
  const char *entry,
  struct cvmfs_attr **st,
  bool get_hash
) {
  cvmfs_attr_free(*st);
  *st = NULL;
  *st = cvmfs_attr_init();
  int retval = fs->get_stat(fs->context_, entry, *st, get_hash);
  return retval;
}

bool Sync(
  const char *dir,
  struct fs_traversal *src,
  struct fs_traversal *dest,
  int parallel,
  bool recursive
) {
  bool retval = true;

  char **src_dir = NULL;
  size_t src_len = 0;
  size_t src_iter = 0;
  char * src_entry = NULL;
  struct cvmfs_attr *src_st = cvmfs_attr_init();
  src->list_dir(src->context_, dir, &src_dir, &src_len);
  qsort(src_dir, src_len, sizeof(char *),  strcmp_list);

  if (getNext(src, dir, src_dir, &src_entry, NULL)) {
    updateStat(src, src_entry, &src_st, true);
  }

  char **dest_dir = NULL;
  size_t dest_len = 0;
  size_t dest_iter = 0;
  char * dest_entry = NULL;
  struct cvmfs_attr *dest_st = cvmfs_attr_init();
  dest->list_dir(dest->context_, dir, &dest_dir, &dest_len);
  qsort(dest_dir, dest_len, sizeof(char *),  strcmp_list);

  if (getNext(dest, dir, dest_dir, &dest_entry, NULL)) {
    updateStat(dest, dest_entry, &dest_st, false);
  }


  // While both have defined members to compare.
  while (src_entry || dest_entry) {
    // Check if item is present in both
    int cmp = 0;
    if (!src_entry) {
      cmp = 1;
    } else if (!dest_entry) {
      cmp = -1;
    } else {
      cmp = strcmp(src_entry, dest_entry);
    }
    if (cmp == 0) {
      // Compares stats to see if they are equivalent
      if (!cvmfs_attr_cmp(src_st, dest_st, dest)) {
        // If not equal, bring dest up-to-date
        switch (src_st->st_mode & S_IFMT) {
          case S_IFREG:
            {
              // They don't point to the same data, link new data
              const char *src_data;
              src_data = src->get_identifier(src->context_, src_st);
              const char *dest_data;
              dest_data = dest->get_identifier(dest->context_, src_st);

              // Touch is atomic, if it fails something else will write file?
              if (!dest->touch(dest->context_, src_st)) {
                // PUSH TO PIPE
                if (!copy_file(src, src_data, dest, dest_data, parallel)) {
                  LogCvmfs(kLogCvmfs, kLogStderr,
                  "Traversal failed to copy %s->%s", src_data, dest_data);
                  return false;
                }
              }

              // Should probably happen in the copy function as it is parallel
              // Also this needs to be separate from copy_file, the target file
              // could already exist and the link needs to be created anyway.
              if (dest->do_link(dest->context_, dest_entry, dest_data)) {
                LogCvmfs(kLogCvmfs, kLogStderr,
                "Traversal failed to link %s->%s", dest_entry, dest_data);
                return false;
              }
            }
            break;
          case S_IFDIR:
            if (dest->set_meta(dest->context_, src_entry, src_st)) {
              LogCvmfs(kLogCvmfs, kLogStderr,
              "Traversal failed to set_meta %s", src_entry);
              return false;
            }
            if (recursive) {
              if (!Sync(src_entry, src, dest, parallel, recursive)) {
                return false;
              }
            }
            break;
          case S_IFLNK:
            // Should likely copy the source of the symlink target
            if (dest->do_symlink(dest->context_, src_entry,
                             src_st->cvm_symlink, src_st)) {
              LogCvmfs(kLogCvmfs, kLogStderr,
              "Traversal failed to symlink %s->%s",
              src_entry, src_st->cvm_symlink);
              return retval;
            }
            break;
          default:
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Unknown file type for %s : %d",
            src_entry, src_st->st_mode);
            return false;
            break;
        }
      } else {
        // The directory exists and is the same,
        // but we still need to check the children
        if (S_ISDIR(src_st->st_mode) && recursive) {
          if (!Sync(src_entry, src, dest, parallel, recursive)) {
            return false;
          }
        }
      }
      if (getNext(src, dir, src_dir, &src_entry, &src_iter)) {
        updateStat(src, src_entry, &src_st, true);
      }
      if (getNext(dest, dir, dest_dir, &dest_entry, &dest_iter)) {
        updateStat(dest, dest_entry, &dest_st, false);
      }

    /* Src contains something missing from Dest */
    } else if (cmp < 0) {
       switch (src_st->st_mode & S_IFMT) {
        case S_IFREG:
            {
              // They don't point to the same data, link new data
              const char *dest_data;
              dest_data = dest->get_identifier(dest->context_, src_st);
              const char *src_data;
              src_data  = src->get_identifier(src->context_, src_st);

              if (!dest->touch(dest->context_, src_st)) {
                if (!copy_file(src, src_data, dest, dest_data, parallel)) {
                  LogCvmfs(kLogCvmfs, kLogStderr,
                  "Traversal failed to copy %s->%s", src_data, dest_data);
                  return false;
                }
              }

              // Should probably happen in the copy function as it is parallel
              // Also this needs to be separate from copy_file, the target file
              // could already exist and the link needs to be created anyway.
              if (dest->do_link(dest->context_, src_entry, dest_data)) {
                LogCvmfs(kLogCvmfs, kLogStderr,
                "Traversal failed to link %s->%s", src_entry, dest_data);
                return false;
              }
            }
          break;
        case S_IFDIR:
          if (dest->do_mkdir(dest->context_, src_entry, src_st)) {
              LogCvmfs(kLogCvmfs, kLogStderr,
              "Traversal failed to mkdir %s", src_entry);
            return false;
          }
          if (recursive) {
            if (!Sync(src_entry, src, dest, parallel, recursive)) {
              return false;
            }
          }
          break;
        case S_IFLNK:
          // Should be same as IFREG? Does link create the file?
          if (dest->do_symlink(dest->context_, src_entry,
                           src_st->cvm_symlink, src_st)) {
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Traversal failed to symlink %s->%s",
            src_entry, src_st->cvm_symlink);
            return false;
          }
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr,
          "Unknown file type for %s : %d",
          src_entry, src_st->st_mode);
          return false;
          break;
      }
      if (getNext(src, dir, src_dir, &src_entry, &src_iter)) {
        updateStat(src, src_entry, &src_st, true);
      }
    /* Dest contains something missing from Src */
    } else if (cmp > 0) {
      switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          if (dest->do_unlink(dest->context_, dest_entry)) {
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Failed to unlink file %s", dest_entry);
            return false;
          }
          break;
        case S_IFDIR:
          // We may want this to be recursive regardless
          /* NOTE(steuber): Do we want this?
          if (recursive) {
            if (!Sync(dest_entry, src, dest, parallel, recursive)) {
              return false;
            }
          }*/
          if (dest->do_rmdir(dest->context_, dest_entry)) {
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Failed to remove directory %s", dest_entry);
            return false;
          }
          break;
        default:
          // Unknown file type, should print error (what stream? log?)
          LogCvmfs(kLogCvmfs, kLogStderr,
          "Unknown file type for %s : %d",
          dest_entry, dest_st->st_mode);
          return false;
          break;
      }
      if (getNext(dest, dir, dest_dir, &dest_entry, &dest_iter)) {
        updateStat(dest, dest_entry, &dest_st, false);
      }
    }
  }
  return true;
}

bool copyFile(
  struct fs_traversal *src_fs,
  const char *src_name,
  struct fs_traversal *dest_fs,
  const char *dest_name
) {
  int retval;

  void *src  = src_fs->get_handle(src_fs->context_, src_name);
  void *dest = dest_fs->get_handle(dest_fs->context_, dest_name);

  retval = src_fs->do_fopen(src, fs_open_read);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed open src : %s\n", src_name);
    return false;
  }

  retval = dest_fs->do_fopen(dest, fs_open_write);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed open dest : %s\n", dest_name);
    return false;
  }

  while (1) {
    char buffer[COPY_BUFFER_SIZE];

    size_t actual_read = 0;
    retval = src_fs->do_fread(src, buffer, COPY_BUFFER_SIZE, &actual_read);
    LogCvmfs(kLogCvmfs, kLogDebug, "Read : %"PRIu64"\n", actual_read);
    if (retval != 0) {
      return false;
    }

    retval = dest_fs->do_fwrite(dest, buffer, actual_read);
    if (retval != 0) {
      return false;
    }
    LogCvmfs(kLogCvmfs, kLogDebug, "Write : %"PRIu64"\n", actual_read);

    if (actual_read < COPY_BUFFER_SIZE) {
      break;
    }
  }

  retval = src_fs->do_fclose(src);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed close src : %s\n", src_name);
    return false;
  }

  retval = dest_fs->do_fclose(dest);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed close dest : %s\n", dest_name);
    return false;
  }

  return true;
}

struct MainWorkerContext {
  struct fs_traversal *src_fs;
  struct fs_traversal *dest_fs;
  int parallel;
};

static void *MainWorker(void *data) {
  MainWorkerContext *mwc = static_cast<MainWorkerContext*>(data);

  while (1) {
    FileCopy next_copy;
    pthread_mutex_lock(&lock_pipe);
    ReadPipe(pipe_chunks[0], &next_copy, sizeof(next_copy));
    pthread_mutex_unlock(&lock_pipe);
    if (next_copy.IsTerminateJob())
      break;

    if (!copyFile(mwc->src_fs, next_copy.src, mwc->dest_fs, next_copy.dest)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
      "File %s failed to copy\n", next_copy.src);
    }

    atomic_dec64(&copy_queue);
  }
  return NULL;
}

// Dummy function for now
bool trim_trace_spec(string *entry) {
  return true;
}

bool copy_file(
  struct fs_traversal *src_fs,
  const char *src_data,
  struct fs_traversal *dest_fs,
  const char *dest_data,
  int parallel
) {
  if (parallel) {
    FileCopy next_copy(src_data, dest_data);
    WritePipe(pipe_chunks[1], &next_copy, sizeof(next_copy));
    atomic_inc64(&copy_queue);
  } else {
    if (!copyFile(src_fs, src_data, dest_fs, dest_data)) {
      printf("File %s failed to copy\n\n", src_data);
      return false;
    }
  }
  return true;
}

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
       "CernVM File System Shrinkwrapper, version %s\n\n"
       "This tool takes a cvmfs cache directory and  outputs\n"
       "to a destination files system for export.\n"
       "Usage: cvmfs_shrinkwrap "
       "[-s][-r][-c][-d][-x][-y][-t|b][-j #threads] <cache directory>\n"
           "Options:\n"
           "  -s Source Filesystem type [default:cvmfs]\n"
           "  -r Source repo\n"
           "  -c Source data\n"
           "  -d Dest type\n"
           "  -x Dest repo\n"
           "  -y Dest data\n"
           "  -t Trace file to be replicated to destination\n"
           "  -b Base directory to be copied\n"
           "  -r Number of retries on copying file [default:0]\n"
           "  -j number of concurrent integrity check worker threads\n",
           VERSION);
}


int Main(int argc, char **argv) {
  // The starting location for the traversal in src
  // Default value is the base directory (only used if not trace provided)
  char *src_repo = NULL;
  char *src_data = NULL;
  char *src_type = NULL;

  char *dest_repo = NULL;
  char *dest_data = NULL;
  char *dest_type = NULL;

  char *base = NULL;
  char *trace_file = NULL;

  int c;
  while ((c = getopt(argc, argv, "hb:s:r:c:d:x:y:t:j:n:")) != -1) {
    switch (c) {
      case 'h':
        shrinkwrap::Usage();
        return kErrorOk;
      case 'b':
        base = strdup(optarg);
        if (trace_file) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "Only allowed to specify either base dir or trace file");
          return kErrorUsage;
        }
        break;
      case 's':
        src_type = strdup(optarg);
        break;
      case 'r':
        src_repo = strdup(optarg);
        break;
      case 'c':
        src_data = strdup(optarg);
        break;
      case 'd':
        dest_type = strdup(optarg);
        break;
      case 'x':
        dest_repo = strdup(optarg);
        break;
      case 'y':
        dest_data = strdup(optarg);
        break;
      case 't':
        trace_file = strdup(optarg);
        if (base) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "Only allowed to specify either base dir or trace file");
          return kErrorUsage;
        }
        break;
      case 'j':
        num_parallel = atoi(optarg);
        if (num_parallel < 1) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "There is at least one worker thread required");
          return kErrorUsage;
        }
        break;
      case 'n':
        retries = atoi(optarg);
        break;
      case '?':
      default:
        shrinkwrap::Usage();
        return kErrorUsage;
    }
  }

  struct fs_traversal *src = FindInterface(src_type);
  if (!src) {
    return 1;
  }
  src->context_ = src->initialize(src_repo, src_data);

  struct fs_traversal *dest = FindInterface(dest_type);
  if (!dest) {
    return 1;
  }
  dest->context_ = dest->initialize(dest_repo, dest_data);

  int result = 1;

  // Initialization
  atomic_init64(&overall_copies);
  atomic_init64(&overall_new);
  atomic_init64(&copy_queue);

  pthread_t *workers =
    reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * num_parallel));

  // Start Workers
  MakePipe(pipe_chunks);
  LogCvmfs(kLogCvmfs, kLogStdout, "Starting %u workers", num_parallel);
  MainWorkerContext mwc;
  mwc.src_fs = src;
  mwc.dest_fs = dest;
  mwc.parallel = num_parallel;

  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_create(&workers[i], NULL, MainWorker,
                                static_cast<void*>(&mwc));
    assert(retval == 0);
  }

  if (!trace_file) {
    ifstream trace(trace_file);
    std::string entry;
    while (getline(trace, entry))
    {
      // Function removes special characters and determines if its recursive
      recursive = trim_trace_spec(&entry);
      char *entry_point = strdup(entry.c_str());
      result = Sync(entry_point, src, dest, num_parallel, recursive);
      free(entry_point);
    }
  } else {
    char *entry_point = strdup(base);
    result = Sync(entry_point, src, dest, num_parallel, true);
    free(entry_point);
  }
  while (atomic_read64(&copy_queue) != 0) {
    SafeSleepMs(100);
  }


  LogCvmfs(kLogCvmfs, kLogStdout, "Stopping %u workers", num_parallel);
  for (unsigned i = 0; i < num_parallel; ++i) {
    FileCopy terminate_workers;
    WritePipe(pipe_chunks[1], &terminate_workers, sizeof(terminate_workers));
  }
  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_join(workers[i], NULL);
    assert(retval == 0);
  }
  ClosePipe(pipe_chunks);

  return result;
}

}  // namespace shrinkwrap

