/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <time.h>

#include <fstream>

#include "atomic.h"
#include "fs_traversal.h"
#include "fs_traversal_interface.h"
#include "fs_traversal_libcvmfs.h"
#include "fs_traversal_posix.h"
#include "libcvmfs.h"
#include "logging.h"
#include "smalloc.h"
#include "spec_tree.h"
#include "statistics.h"
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
  FileCopy()
    : src(NULL)
    , dest(NULL) {}

  FileCopy(char *src, char *dest)
    : src(src)
    , dest(dest) {}

  bool IsTerminateJob() const {
    return ((src == NULL) && (dest == NULL));
  }

  char *src;
  char *dest;
};

unsigned             num_parallel;
bool                 recursive = true;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
unsigned             retries = 1;
atomic_int64         overall_copies;
atomic_int64         overall_new;
atomic_int64         copy_queue;

SpecTree             *spec_tree_ = new SpecTree('*');

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
  int parallel,
  perf::Statistics *pstats);

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

struct fs_dir {
  struct fs_traversal *fs;
  char **dir;
  size_t len;
  ssize_t iter;
  char *entry;
  struct cvmfs_attr *stat;
};

struct fs_dir* fs_dir_init(struct fs_traversal *fs)
{
  struct fs_dir *dir = new struct fs_dir;
  dir->fs = fs;
  dir->dir = NULL;
  dir->len = 0;
  dir->iter = -1;
  dir->entry = NULL;
  dir->stat = cvmfs_attr_init();
  return dir;
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
  if (retval != 0) {
    return false;
  }
  return true;
}

bool getNext(
  struct fs_traversal *fs,
  const char *dir,
  char **dir_list,
  char **entry,
  ssize_t *iter,
  struct cvmfs_attr **st,
  bool get_hash,
  bool is_src,
  perf::Statistics *pstats
) {
  size_t location = 0;
  if (iter) {
    location = (*iter)+1;
    *iter = location;
  }

  free(*entry);
  *entry = NULL;

  if (dir_list && dir_list[location]) {
    *entry = get_full_path(dir, dir_list[location]);
    if (entry && is_src && !spec_tree_->IsMatching(string(*entry))) {
      return getNext(fs, dir, dir_list, entry, iter,
                     st, get_hash, is_src, pstats);
    }
    if (entry && !updateStat(fs, *entry, st, get_hash)) {
      return getNext(fs, dir, dir_list, entry, iter,
                     st, get_hash, is_src, pstats);
    }
  } else {
    return false;
  }
  if (is_src) {
    pstats->Lookup(SHRINKWRAP_STAT_SRC_ENTRIES)->Inc();
  } else {
    pstats->Lookup(SHRINKWRAP_STAT_DEST_ENTRIES)->Inc();
  }
  return true;
}

void list_src_dir(
  struct fs_traversal *src,
  const char *dir,
  char ***buf,
  size_t *len
) {
  int retval = spec_tree_->ListDir(dir, buf, len);

  if (retval == SPEC_READ_FS) {
    src->list_dir(src->context_, dir, buf, len);
    qsort(*buf, *len, sizeof(char *),  strcmp_list);
  }
}

bool Sync(
  const char *dir,
  struct fs_traversal *src,
  struct fs_traversal *dest,
  int parallel,
  bool recursive,
  perf::Statistics *pstats
) {
  bool retval = true;

  char **src_dir = NULL;
  size_t src_len = 0;
  ssize_t src_iter = -1;
  char * src_entry = NULL;
  struct cvmfs_attr *src_st = cvmfs_attr_init();
  list_src_dir(src, dir, &src_dir, &src_len);

  getNext(src, dir, src_dir, &src_entry, &src_iter,
          &src_st, true, true, pstats);

  char **dest_dir = NULL;
  size_t dest_len = 0;
  ssize_t dest_iter = -1;
  char * dest_entry = NULL;
  struct cvmfs_attr *dest_st = cvmfs_attr_init();
  dest->list_dir(dest->context_, dir, &dest_dir, &dest_len);
  qsort(dest_dir, dest_len, sizeof(char *),  strcmp_list);

  getNext(dest, dir, dest_dir, &dest_entry, &dest_iter,
          &dest_st, false, false, pstats);

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
//              const char *src_data;
//              src_data = src->get_identifier(src->context_, src_st);
              const char *dest_data;
              dest_data = dest->get_identifier(dest->context_, src_st);

              // Touch is atomic, if it fails something else will write file?
              if (!dest->touch(dest->context_, src_st)) {
                pstats->Lookup(SHRINKWRAP_STAT_DEST_ENTRIES)->Inc();
                // PUSH TO PIPE
                if (!copy_file(src, src_entry, dest,
                               dest_data, parallel, pstats)) {
                  LogCvmfs(kLogCvmfs, kLogStderr,
                  "Traversal failed to copy %s->%s", src_entry, dest_data);
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
            if (dest->set_meta(dest->context_, src_entry, src_st)) {
              LogCvmfs(kLogCvmfs, kLogStderr,
              "Traversal failed to set_meta %s", src_entry);
              return false;
            }
            if (recursive) {
              if (!Sync(src_entry, src, dest, parallel,
                recursive, pstats)) {
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
          if (!Sync(src_entry, src, dest, parallel, recursive, pstats)) {
            return false;
          }
        }
      }
      getNext(src, dir, src_dir, &src_entry, &src_iter,
              &src_st, true, true, pstats);
      getNext(dest, dir, dest_dir, &dest_entry, &dest_iter,
              &dest_st, false, false, pstats);
    /* Src contains something missing from Dest */
    } else if (cmp < 0) {
       switch (src_st->st_mode & S_IFMT) {
        case S_IFREG:
            {
              // They don't point to the same data, link new data
              const char *dest_data;
              dest_data = dest->get_identifier(dest->context_, src_st);
//              const char *src_data;
//              src_data  = src->get_identifier(src->context_, src_st);

              if (!dest->touch(dest->context_, src_st)) {
                pstats->Lookup(SHRINKWRAP_STAT_DEST_ENTRIES)->Inc();
                if (!copy_file(src, src_entry, dest,
                               dest_data, parallel, pstats)) {
                  LogCvmfs(kLogCvmfs, kLogStderr,
                  "Traversal failed to copy %s->%s", src_entry, dest_data);
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
            pstats->Lookup(SHRINKWRAP_STAT_DEST_ENTRIES)->Inc();
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Traversal failed to mkdir %s", src_entry);
            return false;
          }
          if (recursive) {
            if (!Sync(src_entry, src, dest, parallel, recursive, pstats)) {
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
      getNext(src, dir, src_dir, &src_entry, &src_iter,
              &src_st, true, true, pstats);
    /* Dest contains something missing from Src */
    } else if (cmp > 0) {
      switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          if (!dest->do_unlink(dest->context_, dest_entry)) {
            LogCvmfs(kLogCvmfs, kLogStderr,
            "Failed to unlink file %s", dest_entry);
            return false;
          }
          break;
        case S_IFDIR:
          // We may want this to be recursive regardless
          /* NOTE(steuber): Do we want this?
          if (recursive) {
            if (!Sync(dest_entry, src, dest, parallel, recursive, pstats)) {
              return false;
            }
          }*/
          if (!dest->do_rmdir(dest->context_, dest_entry)) {
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
      getNext(dest, dir, dest_dir, &dest_entry, &dest_iter,
              &dest_st, false, false, pstats);
    }
  }
  return true;
}

bool copyFile(
  struct fs_traversal *src_fs,
  const char *src_name,
  struct fs_traversal *dest_fs,
  const char *dest_name,
  perf::Statistics *pstats
) {
  int retval;

  void *src  = src_fs->get_handle(src_fs->context_, src_name);
  void *dest = dest_fs->get_handle(dest_fs->context_, dest_name);

  retval = src_fs->do_fopen(src, fs_open_read);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr,
    "Failed open src : %s : %d : %s\n",
    src_name, errno, strerror(errno));
    return false;
  }

  retval = dest_fs->do_fopen(dest, fs_open_write);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr,
    "Failed open dest : %s : %d : %s\n",
    dest_name, errno, strerror(errno));
    return false;
  }
  size_t bytes_transferred = 0;
  while (1) {
    char buffer[COPY_BUFFER_SIZE];

    size_t actual_read = 0;
    retval = src_fs->do_fread(src, buffer, sizeof(buffer), &actual_read);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
      "Read failed : %d %s\n", errno, strerror(errno));
      return false;
    }
    bytes_transferred+=actual_read;
    retval = dest_fs->do_fwrite(dest, buffer, actual_read);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
      "Write failed : %d %s\n", errno, strerror(errno));
      return false;
    }

    if (actual_read < COPY_BUFFER_SIZE) {
      break;
    }
  }
  pstats->Lookup(SHRINKWRAP_STAT_BYTE_COUNT)->Xadd(bytes_transferred);

  retval = src_fs->do_fclose(src);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr,
    "Failed close src : %s : %d : %s\n",
    src_name, errno, strerror(errno));
    return false;
  }

  retval = dest_fs->do_fclose(dest);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr,
    "Failed close dest : %s : %d : %s\n",
    dest_name, errno, strerror(errno));
    return false;
  }

  return true;
}

struct MainWorkerContext {
  struct fs_traversal *src_fs;
  struct fs_traversal *dest_fs;
  perf::Statistics *pstats;
  int parallel;
};

struct MainWorkerSpecificContext {
  struct MainWorkerContext *mwc;
  int num_thread;
};

static void *MainWorker(void *data) {
  MainWorkerSpecificContext *sc = static_cast<MainWorkerSpecificContext*>(data);
  MainWorkerContext *mwc = sc->mwc;
  perf::Counter *files_transferred
    = mwc->pstats->Lookup(SHRINKWRAP_STAT_FILE_COUNT);
  time_t last_print_time = 0;
  if (sc->num_thread == 0) {
    last_print_time = time(NULL);
  }

  while (1) {
    if (sc->num_thread == 0 && time(NULL)-last_print_time > 10) {
      LogCvmfs(kLogCvmfs, kLogStdout,
        "%s",
        mwc->pstats->PrintList(perf::Statistics::kPrintSimple).c_str());
      last_print_time = time(NULL);
    }
    FileCopy next_copy;
    pthread_mutex_lock(&lock_pipe);
    ReadPipe(pipe_chunks[0], &next_copy, sizeof(next_copy));
    pthread_mutex_unlock(&lock_pipe);
    if (next_copy.IsTerminateJob())
      break;

    if (!next_copy.src || !next_copy.dest) {
      continue;
    }

    if (!copyFile(mwc->src_fs, next_copy.src, mwc->dest_fs,
                  next_copy.dest, mwc->pstats)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
      "File %s failed to copy\n", next_copy.src);
    }
    files_transferred->Inc();

    atomic_dec64(&copy_queue);
  }
  return NULL;
}

bool copy_file(
  struct fs_traversal *src_fs,
  const char *src_data,
  struct fs_traversal *dest_fs,
  const char *dest_data,
  int parallel,
  perf::Statistics *pstats
) {
  if (parallel) {
    FileCopy next_copy(strdup(src_data), strdup(dest_data));
    WritePipe(pipe_chunks[1], &next_copy, sizeof(next_copy));
    atomic_inc64(&copy_queue);
  } else {
    if (!copyFile(src_fs, src_data, dest_fs, dest_data, pstats)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
      "File %s failed to copy\n\n", src_data);
      return false;
    }
    pstats->Lookup(SHRINKWRAP_STAT_FILE_COUNT)->Inc();
  }
  return true;
}

perf::Statistics *GetSyncStatTemplate() {
  perf::Statistics *result = new perf::Statistics();
  result->Register(SHRINKWRAP_STAT_BYTE_COUNT,
    "The number of bytes transfered from the source to the destination");
  result->Register(SHRINKWRAP_STAT_FILE_COUNT,
    "The number of files transfered from the source to the destination");
  result->Register(SHRINKWRAP_STAT_SRC_ENTRIES,
    "The number of file system entries processed in the source");
  result->Register(SHRINKWRAP_STAT_DEST_ENTRIES,
    "The number of file system entries processed in the destination");
  return result;
}

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
       "CernVM File System Shrinkwrapper, version %s\n\n"
       "This tool takes a cvmfs cache directory and  outputs\n"
       "to a destination files system for export.\n"
       "Usage: cvmfs_shrinkwrap "
       "[-s][-r][-c][-f][-d][-x][-y][-z][-t|b][-j #threads]\n"
           "Options:\n"
           "  -s Source Filesystem type [default:cvmfs]\n"
           "  -r Source repo\n"
           "  -c Source cache\n"
           "  -f Source config\n"
           "  -d Dest type\n"
           "  -x Dest repo\n"
           "  -y Dest cache\n"
           "  -z Dest config\n"
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
  char *src_cache = NULL;
  char *src_type = NULL;
  char *src_config = NULL;

  char *dest_repo = NULL;
  char *dest_cache = NULL;
  char *dest_type = NULL;
  char *dest_config = NULL;

  char *base = NULL;
  char *spec_file = NULL;

  int c;
  while ((c = getopt(argc, argv, "hb:s:r:c:f:d:x:y:t:j:n:")) != -1) {
    switch (c) {
      case 'h':
        shrinkwrap::Usage();
        return kErrorOk;
      case 'b':
        base = strdup(optarg);
        if (spec_file) {
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
        src_cache = strdup(optarg);
        break;
      case 'f':
        src_config = strdup(optarg);
        break;
      case 'd':
        dest_type = strdup(optarg);
        break;
      case 'x':
        dest_repo = strdup(optarg);
        break;
      case 'y':
        dest_cache = strdup(optarg);
        break;
      case 'z':
        dest_config = strdup(optarg);
        break;
      case 't':
        spec_file = strdup(optarg);
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

  if (!src_type) {
    src_type = strdup("cvmfs");
  }

  if (!dest_type) {
    dest_type = strdup("posix");
  }

  if (!dest_repo) {
    dest_repo = strdup("/tmp/cvmfs/");
  }

  if (!dest_cache) {
    size_t len = 7 + strlen(dest_repo);
    dest_cache = reinterpret_cast<char *>(malloc(len*sizeof(char)));
    snprintf(dest_cache, len, "%s/.data",  dest_repo);
  }


  struct fs_traversal *src = FindInterface(src_type);
  if (!src) {
    return 1;
  }
  src->context_ = src->initialize(src_repo, src_cache, src_config);

  struct fs_traversal *dest = FindInterface(dest_type);
  if (!dest) {
    return 1;
  }
  dest->context_ = dest->initialize(dest_repo, dest_cache, dest_config);

  perf::Statistics *pstats = GetSyncStatTemplate();

  // Initialization
  atomic_init64(&overall_copies);
  atomic_init64(&overall_new);
  atomic_init64(&copy_queue);

  pthread_t *workers = NULL;

  struct MainWorkerSpecificContext *specificWorkerContexts = NULL;

  MainWorkerContext *mwc = NULL;

  if (num_parallel > 0) {
    workers =
    reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * num_parallel));

    specificWorkerContexts =
      reinterpret_cast<struct MainWorkerSpecificContext *>(
        smalloc(sizeof(struct MainWorkerSpecificContext) * num_parallel));

    mwc = new struct MainWorkerContext;
    // Start Workers
    MakePipe(pipe_chunks);
    LogCvmfs(kLogCvmfs, kLogStdout, "Starting %u workers", num_parallel);
    MainWorkerContext *mwc = new MainWorkerContext();
    mwc->src_fs = src;
    mwc->dest_fs = dest;
    mwc->pstats = pstats;
    mwc->parallel = num_parallel;

    for (unsigned i = 0; i < num_parallel; ++i) {
      specificWorkerContexts[i].mwc = mwc;
      specificWorkerContexts[i].num_thread = i;
      int retval = pthread_create(&workers[i], NULL, MainWorker,
          static_cast<void*>(&(specificWorkerContexts[i])));
      assert(retval == 0);
    }
  }

  if (!base) {
    base = strdup("");
  }

  if (spec_file) {
    delete spec_tree_;
    spec_tree_ = SpecTree::Create(spec_file);
  }

  char *entry_point = strdup(base);
  int result =
    !Sync(entry_point, src, dest, num_parallel, recursive, pstats);
  free(entry_point);

  while (atomic_read64(&copy_queue) != 0) {
    SafeSleepMs(100);
  }


  if (num_parallel > 0) {
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
    delete workers;
    delete specificWorkerContexts;
    delete mwc;
  }
  LogCvmfs(kLogCvmfs, kLogStdout,
        "%s",
        pstats->PrintList(perf::Statistics::kPrintHeader).c_str());
  delete pstats;

  src->finalize(src->context_);
  dest->finalize(dest->context_);

  return result;
}

}  // namespace shrinkwrap

