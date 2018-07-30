/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <time.h>

#include <fstream>
#include <vector>

#include "atomic.h"
#include "cvmfs_config.h"
#include "export_plugin/fs_traversal.h"
#include "export_plugin/fs_traversal_interface.h"
#include "export_plugin/fs_traversal_libcvmfs.h"
#include "export_plugin/posix/interface.h"
#include "export_plugin/spec_tree.h"
#include "libcvmfs.h"
#include "logging.h"
#include "smalloc.h"
#include "statistics.h"
#include "util/posix.h"
#include "util/string.h"

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

class RecDir {
 public:
  RecDir()
    : dir(NULL)
    , recursive(false) {}

  RecDir(char *dir, bool recursive)
    : dir(dir)
    , recursive(recursive) {}

  ~RecDir() {
    free(dir);
  }

  bool IsTerminateJob() const {
    return (dir == NULL);
  }

  char *dir;
  bool recursive;
};

unsigned             num_parallel_ = 0;
bool                 recursive = true;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
atomic_int64         overall_copies;
atomic_int64         overall_new;
atomic_int64         copy_queue;

vector<RecDir*>      dirs_;

unsigned             retries_ = 0;

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

bool cvmfs_attr_cmp(struct cvmfs_attr *src, struct cvmfs_attr *dest,
    struct fs_traversal *dest_fs) {
  if (!src) { return false; }
  if (!dest) { return false; }
  if (src->version != dest->version) { return false; }
  if (src->size != dest->size) { return false; }

  // Actual contents of stat, mapped from DirectoryEntry
  if ( (!S_ISLNK(src->st_mode) && src->st_mode != dest->st_mode)
      || ((S_IFMT & src->st_mode) != (S_IFMT & dest->st_mode)) )
    { return false; }

  if (!S_ISLNK(src->st_mode) && src->st_uid != dest->st_uid)
    { return false; }
  if (!S_ISLNK(src->st_mode) && src->st_gid != dest->st_gid)
    { return false; }


  // CVMFS related content
  if (S_ISREG(src->st_mode) && src->cvm_checksum) {
    if (!dest_fs->is_hash_consistent(dest_fs->context_, src)) { return false; }
  }

  if (S_ISLNK(src->st_mode)) {
    if (strcmp(src->cvm_name, dest->cvm_name)) { return false; }
  } else if ((!S_ISLNK(src->st_mode) && S_ISLNK(dest->st_mode)) ||
            (S_ISLNK(src->st_mode) && !S_ISLNK(dest->st_mode))) {
    return false;
  }

  if (src->cvm_name != NULL && dest->cvm_name != NULL) {
    if (strcmp(src->cvm_name, dest->cvm_name)) { return false; }
  } else if ((!src->cvm_name && dest->cvm_name) ||
            (src->cvm_name && !dest->cvm_name)) {
    return false;
  }

  // TODO(nhazekam): Not comparing Xattrs yet
  // void *       cvm_xattrs;
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
  src_fs->do_ffree(src);

  retval = dest_fs->do_fclose(dest);
  if (retval != 0) {
    // Handle error
    LogCvmfs(kLogCvmfs, kLogStderr,
    "Failed close dest : %s : %d : %s\n",
    dest_name, errno, strerror(errno));
    return false;
  }
  dest_fs->do_ffree(dest);

  return true;
}

char *get_full_path(const char *dir, const char *entry) {
  size_t len = 2 + strlen(dir)+ strlen(entry);
  char * path = reinterpret_cast<char *>(malloc(len));
  snprintf(path, len, "%s/%s",  dir, entry);
  return path;
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

bool handle_file(
  struct fs_traversal *src,
  struct cvmfs_attr *src_st,
  struct fs_traversal *dest,
  struct cvmfs_attr *dest_st,
  const char *entry,
  perf::Statistics *pstats
) {
  bool result = true;
  // They don't point to the same data, link new data
  char *dest_data = dest->get_identifier(dest->context_, src_st);

  // Touch is atomic, if it fails something else will write file
  if (!dest->touch(dest->context_, src_st)) {
    if (num_parallel_) {
      FileCopy next_copy(strdup(entry), strdup(dest_data));
      WritePipe(pipe_chunks[1], &next_copy, sizeof(next_copy));
      atomic_inc64(&copy_queue);
    } else {
      if (!copyFile(src, entry, dest, dest_data, pstats)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
          "Failed to copy %s->%s : %d : %s",
          entry, dest_data, errno, strerror(errno));
        errno = 0;
        result = false;
      }
      pstats->Lookup(SHRINKWRAP_STAT_FILE_COUNT)->Inc();
    }
  }

  // Should probably happen in the copy function as it is parallel
  // Also this needs to be separate from copyFile, the target file
  // could already exist and the link needs to be created anyway.
  if (result && dest->do_link(dest->context_, entry, dest_data)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
      "Failed to link %s->%s : %d : %s",
      entry, dest_data, errno, strerror(errno));
    errno = 0;
    result = false;
  }
  free(dest_data);
  return result;
}

bool handle_dir(
  struct fs_traversal *src,
  struct cvmfs_attr *src_st,
  struct fs_traversal *dest,
  struct cvmfs_attr *dest_st,
  const char *entry
) {
  if (dest->do_mkdir(dest->context_, entry, src_st)) {
    if (errno == EEXIST) {
      errno = 0;
      if (dest->set_meta(dest->context_, entry, src_st)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
          "Traversal failed to set_meta %s", entry);
        return false;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr,
        "Traversal failed to mkdir %s : %d : %s",
        entry, errno, strerror(errno));
      return false;
    }
  }
  return true;
}

void add_dir_for_sync(const char *dir, bool recursive) {
  dirs_.push_back(new RecDir(strdup(dir), recursive));
}

bool Sync(
  const char *dir,
  struct fs_traversal *src,
  struct fs_traversal *dest,
  bool recursive,
  perf::Statistics *pstats
) {
  int cmp = 0;

  char **src_dir = NULL;
  size_t src_len = 0;
  ssize_t src_iter = -1;
  char * src_entry = NULL;
  struct cvmfs_attr *src_st = cvmfs_attr_init();
  list_src_dir(src, dir, &src_dir, &src_len);

  char **dest_dir = NULL;
  size_t dest_len = 0;
  ssize_t dest_iter = -1;
  char * dest_entry = NULL;
  struct cvmfs_attr *dest_st = cvmfs_attr_init();
  dest->list_dir(dest->context_, dir, &dest_dir, &dest_len);
  qsort(dest_dir, dest_len, sizeof(char *),  strcmp_list);

  // While both have defined members to compare.
  while (1) {
    if (cmp <= 0) {
      getNext(src, dir, src_dir, &src_entry, &src_iter,
            &src_st, true, true, pstats);
    }

    if (cmp >= 0) {
      getNext(dest, dir, dest_dir, &dest_entry, &dest_iter,
              &dest_st, false, false, pstats);
    } else {
      // A destination entry was added
      pstats->Lookup(SHRINKWRAP_STAT_DEST_ENTRIES)->Inc();
    }

    if (!src_entry && !dest_entry) {
      // If we have visited all entries in both we break
      break;
    } else if (!src_entry) {
      cmp = 1;
    } else if (!dest_entry) {
      cmp = -1;
    } else {
      cmp = strcmp(src_entry, dest_entry);
    }

    if (cmp <= 0) {
      // Compares stats to see if they are equivalent
      if (cmp == 0 && cvmfs_attr_cmp(src_st, dest_st, dest)) {
        if (S_ISDIR(src_st->st_mode) && recursive) {
          add_dir_for_sync(src_entry, recursive);
        }
        continue;
      }
      // If not equal, bring dest up-to-date
      switch (src_st->st_mode & S_IFMT) {
        case S_IFREG:
          if (!handle_file(src, src_st, dest, dest_st, src_entry, pstats))
            return false;
          break;
        case S_IFDIR:
          if (!handle_dir(src, src_st, dest, dest_st, src_entry))
            return false;
          if (recursive)
            add_dir_for_sync(src_entry, recursive);
          break;
        case S_IFLNK:
          // Should likely copy the source of the symlink target
          if (dest->do_symlink(dest->context_, src_entry,
                               src_st->cvm_symlink, src_st) != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr,
              "Traversal failed to symlink %s->%s : %d : %s",
              src_entry, src_st->cvm_symlink, errno, strerror(errno));
            return false;
          }
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr,
            "Unknown file type for %s : %d", src_entry, src_st->st_mode);
          return false;
      }
    /* Dest contains something missing from Src */
    } else {
      switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          if (dest->do_unlink(dest->context_, dest_entry) != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr,
              "Failed to unlink file %s", dest_entry);
            return false;
          }
          break;
        case S_IFDIR:
          // We may want this to be recursive regardless
          if (recursive) {
            add_dir_for_sync(src_entry, recursive);
          }
          if (dest->do_rmdir(dest->context_, dest_entry) != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr,
              "Failed to remove directory %s", dest_entry);
            return false;
          }
          break;
        default:
          // Unknown file type, should print error (what stream? log?)
          LogCvmfs(kLogCvmfs, kLogStderr,
            "Unknown file type for %s : %d", dest_entry, dest_st->st_mode);
          return false;
      }
    }
  }

  cvmfs_list_free(src_dir);
  free(src_entry);
  if (src_st)
    cvmfs_attr_free(src_st);

  cvmfs_list_free(dest_dir);
  free(dest_entry);
  if (dest_st)
    cvmfs_attr_free(dest_st);

  return true;
}

bool SyncFull(
  struct fs_traversal *src,
  struct fs_traversal *dest,
  perf::Statistics *pstats
) {
  while (!dirs_.empty()) {
    RecDir *next_dir = dirs_.back();
    dirs_.pop_back();
    if (next_dir->IsTerminateJob())
      break;

    if (!Sync(next_dir->dir, src, dest, next_dir->recursive, pstats)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
        "File %s failed to copy\n", next_dir->dir);
      return false;
    }

    delete next_dir;
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

    free(next_copy.src);
    free(next_copy.dest);

    atomic_dec64(&copy_queue);
  }
  return NULL;
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

int SyncInit(
  struct fs_traversal *src,
  struct fs_traversal *dest,
  const char *base,
  const char *spec,
  unsigned parallel,
  unsigned retries
) {
  num_parallel_ = parallel;
  retries_ = retries;

  perf::Statistics *pstats = GetSyncStatTemplate();

  // Initialization
  atomic_init64(&overall_copies);
  atomic_init64(&overall_new);
  atomic_init64(&copy_queue);

  pthread_t *workers = NULL;

  struct MainWorkerSpecificContext *specificWorkerContexts = NULL;

  MainWorkerContext *mwc = NULL;

  if (num_parallel_ > 0) {
    workers =
    reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * num_parallel_));

    specificWorkerContexts =
      reinterpret_cast<struct MainWorkerSpecificContext *>(
        smalloc(sizeof(struct MainWorkerSpecificContext) * num_parallel_));

    mwc = new struct MainWorkerContext;
    // Start Workers
    MakePipe(pipe_chunks);
    LogCvmfs(kLogCvmfs, kLogStdout, "Starting %u workers", num_parallel_);
    mwc->src_fs = src;
    mwc->dest_fs = dest;
    mwc->pstats = pstats;
    mwc->parallel = num_parallel_;

    for (unsigned i = 0; i < num_parallel_; ++i) {
      specificWorkerContexts[i].mwc = mwc;
      specificWorkerContexts[i].num_thread = i;
      int retval = pthread_create(&workers[i], NULL, MainWorker,
          static_cast<void*>(&(specificWorkerContexts[i])));
      assert(retval == 0);
    }
  }

  if (spec) {
    delete spec_tree_;
    spec_tree_ = SpecTree::Create(spec);
  }

  add_dir_for_sync(base, recursive);
  int result = !SyncFull(src, dest, pstats);

  while (atomic_read64(&copy_queue) != 0) {
    SafeSleepMs(100);
  }


  if (num_parallel_ > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Stopping %u workers", num_parallel_);
    for (unsigned i = 0; i < num_parallel_; ++i) {
      FileCopy terminate_workers;
      WritePipe(pipe_chunks[1], &terminate_workers, sizeof(terminate_workers));
    }
    for (unsigned i = 0; i < num_parallel_; ++i) {
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

  delete spec_tree_;

  return result;
}

int GarbageCollect(struct fs_traversal *fs) {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "Performing garbage collection...");
  time_t start_time = time(NULL);
  int retval = fs->garbage_collector(fs->context_);
  time_t end_time = time(NULL);
  LogCvmfs(kLogCvmfs, kLogStdout,
    "Garbage collection took %d seconds.",
  (end_time-start_time));
  return retval;
}

}  // namespace shrinkwrap

