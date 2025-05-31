/**
 * This file is part of the CernVM File System.
 */

#include "shrinkwrap/fs_traversal.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>

#include <fstream>
#include <map>
#include <vector>

#include "libcvmfs.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/fs_traversal_libcvmfs.h"
#include "shrinkwrap/posix/interface.h"
#include "shrinkwrap/spec_tree.h"
#include "statistics.h"
#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

// Taken from fsck
enum Errors {
  kErrorOk = 0,
  kErrorFixed = 1,
  kErrorReboot = 2,
  kErrorUnfixed = 4,
  kErrorOperational = 8,
  kErrorUsage = 16,
};

int strcmp_list(const void *a, const void *b) {
  char const **char_a = (char const **)a;
  char const **char_b = (char const **)b;

  return strcmp(*char_a, *char_b);
}

namespace shrinkwrap {

namespace {

// No destructor is written to prevent double free and
// corruption of string pointers. After the copy written
// to the pipe, the new copy falls out of scope and
// is destroyed. If there is a destructor that frees the
// strings they get deleted after writing to the pipe.
class FileCopy {
 public:
  FileCopy() : src(NULL), dest(NULL) { }

  FileCopy(char *src, char *dest) : src(strdup(src)), dest(strdup(dest)) { }

  bool IsTerminateJob() const { return ((src == NULL) && (dest == NULL)); }

  char *src;
  char *dest;
};

class RecDir {
 public:
  RecDir() : dir(NULL), recursive(false) { }

  RecDir(const char *dir, bool recursive)
      : dir(strdup(dir)), recursive(recursive) { }

  ~RecDir() { free(dir); }

  char *dir;
  bool recursive;
};

unsigned num_parallel_ = 0;
bool recursive = true;
uint64_t stat_update_period_ = 0;  // Off for testing
int pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t lock_pipe = PTHREAD_MUTEX_INITIALIZER;
atomic_int64 copy_queue;

vector<RecDir *> dirs_;

SpecTree *spec_tree_ = new SpecTree('*');

}  // namespace

struct fs_traversal *FindInterface(const char *type) {
  if (!strcmp(type, "posix")) {
    return posix_get_interface();
  } else if (!strcmp(type, "cvmfs")) {
    return libcvmfs_get_interface();
  }
  LogCvmfs(kLogCvmfs, kLogStderr, "Unknown File System Interface : %s", type);
  return NULL;
}

bool cvmfs_attr_cmp(struct cvmfs_attr *src, struct cvmfs_attr *dest,
                    struct fs_traversal *dest_fs) {
  if (!src) {
    return false;
  }
  if (!dest) {
    return false;
  }
  if (src->version != dest->version) {
    return false;
  }
  if (src->size != dest->size) {
    return false;
  }

  // Actual contents of stat, mapped from DirectoryEntry
  if ((!S_ISLNK(src->st_mode) && src->st_mode != dest->st_mode)
      || ((S_IFMT & src->st_mode) != (S_IFMT & dest->st_mode))) {
    return false;
  }

  if (!S_ISLNK(src->st_mode) && src->st_uid != dest->st_uid) {
    return false;
  }
  if (!S_ISLNK(src->st_mode) && src->st_gid != dest->st_gid) {
    return false;
  }


  // CVMFS related content
  if (S_ISREG(src->st_mode) && src->cvm_checksum) {
    if (dest->cvm_checksum != NULL) {
      if (strcmp(src->cvm_checksum, dest->cvm_checksum)) {
        return false;
      }
    } else if (!dest_fs->is_hash_consistent(dest_fs->context_, src)) {
      return false;
    }
  }

  if (S_ISLNK(src->st_mode)) {
    if (strcmp(src->cvm_symlink, dest->cvm_symlink)) {
      return false;
    }
  }

  if (strcmp(src->cvm_name, dest->cvm_name)) {
    return false;
  }

  // TODO(nhazekam): Not comparing Xattrs yet
  // void *       cvm_xattrs;
  return true;
}

bool copyFile(struct fs_traversal *src_fs,
              const char *src_name,
              struct fs_traversal *dest_fs,
              const char *dest_name,
              perf::Statistics *pstats) {
  int retval = 0;
  bool status = true;
  size_t bytes_transferred = 0;

  // Get file handles from each respective filesystem
  void *src = src_fs->get_handle(src_fs->context_, src_name);
  void *dest = dest_fs->get_handle(dest_fs->context_, dest_name);

  retval = src_fs->do_fopen(src, fs_open_read);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed open src : %s : %d : %s\n",
             src_name, errno, strerror(errno));
    status = false;
    goto copy_fail;
  }

  retval = dest_fs->do_fopen(dest, fs_open_write);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed open dest : %s : %d : %s\n",
             dest_name, errno, strerror(errno));
    status = false;
    goto copy_fail;
  }

  while (status) {
    char buffer[COPY_BUFFER_SIZE];

    size_t actual_read = 0;
    retval = src_fs->do_fread(src, buffer, sizeof(buffer), &actual_read);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Read failed : %d %s\n", errno,
               strerror(errno));
      status = false;
      goto copy_fail;
    }
    bytes_transferred += actual_read;

    retval = dest_fs->do_fwrite(dest, buffer, actual_read);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Write failed : %d %s\n", errno,
               strerror(errno));
      status = false;
      goto copy_fail;
    }

    if (actual_read < COPY_BUFFER_SIZE) {
      break;
    }
  }

  pstats->Lookup(SHRINKWRAP_STAT_DATA_BYTES)->Xadd(bytes_transferred);

  retval = src_fs->do_fclose(src);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed close src : %s : %d : %s\n",
             src_name, errno, strerror(errno));
    status = false;
    goto copy_fail;
  }

  retval = dest_fs->do_fclose(dest);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed close dest : %s : %d : %s\n",
             dest_name, errno, strerror(errno));
    status = false;
    goto copy_fail;
  }

copy_fail:
  // This function will close the file if still open
  // and free the handle.
  src_fs->do_ffree(src);
  dest_fs->do_ffree(dest);

  return status;
}

char *get_full_path(const char *dir, const char *entry) {
  const size_t len = 2 + strlen(dir) + strlen(entry);
  char *path = reinterpret_cast<char *>(smalloc(len));
  snprintf(path, len, "%s/%s", dir, entry);
  return path;
}

bool updateStat(struct fs_traversal *fs,
                const char *entry,
                struct cvmfs_attr **st,
                bool get_hash) {
  cvmfs_attr_free(*st);
  *st = NULL;
  *st = cvmfs_attr_init();
  const int retval = fs->get_stat(fs->context_, entry, *st, get_hash);
  if (retval != 0) {
    return false;
  }
  return true;
}

bool getNext(struct fs_traversal *fs,
             const char *dir,
             char **dir_list,
             char **entry,
             ssize_t *iter,
             struct cvmfs_attr **st,
             bool get_hash,
             bool is_src,
             perf::Statistics *pstats) {
  size_t location = 0;
  if (iter) {
    location = (*iter) + 1;
    *iter = location;
  }

  free(*entry);
  *entry = NULL;

  if (dir_list && dir_list[location]) {
    *entry = get_full_path(dir, dir_list[location]);
    if (entry && is_src && !spec_tree_->IsMatching(string(*entry))) {
      return getNext(fs, dir, dir_list, entry, iter, st, get_hash, is_src,
                     pstats);
    }
    if (entry && !updateStat(fs, *entry, st, get_hash)) {
      return getNext(fs, dir, dir_list, entry, iter, st, get_hash, is_src,
                     pstats);
    }
  } else {
    return false;
  }
  if (is_src) {
    pstats->Lookup(SHRINKWRAP_STAT_ENTRIES_SRC)->Inc();
    switch ((*st)->st_mode & S_IFMT) {
      case S_IFREG:
        pstats->Lookup(SHRINKWRAP_STAT_COUNT_FILE)->Inc();
        pstats->Lookup(SHRINKWRAP_STAT_COUNT_BYTE)->Xadd((*st)->st_size);
        break;
      case S_IFDIR:
        pstats->Lookup(SHRINKWRAP_STAT_COUNT_DIR)->Inc();
        break;
      case S_IFLNK:
        pstats->Lookup(SHRINKWRAP_STAT_COUNT_SYMLINK)->Inc();
        break;
    }
  } else {
    pstats->Lookup(SHRINKWRAP_STAT_ENTRIES_DEST)->Inc();
  }
  return true;
}

void list_src_dir(struct fs_traversal *src,
                  const char *dir,
                  char ***buf,
                  size_t *len) {
  const int retval = spec_tree_->ListDir(dir, buf, len);

  if (retval == SPEC_READ_FS) {
    src->list_dir(src->context_, dir, buf, len);
    qsort(*buf, *len, sizeof(char *), strcmp_list);
  }
}

bool handle_file(struct fs_traversal *src,
                 struct cvmfs_attr *src_st,
                 struct fs_traversal *dest,
                 struct cvmfs_attr *dest_st,
                 const char *entry,
                 perf::Statistics *pstats) {
  bool result = true;
  // They don't point to the same data, link new data
  char *dest_data = dest->get_identifier(dest->context_, src_st);

  // Touch is atomic, if it fails something else will write file
  if (!dest->touch(dest->context_, src_st)) {
    char *src_ident = src->get_identifier(src->context_, src_st);
    if (num_parallel_) {
      FileCopy next_copy(src_ident, dest_data);

      WritePipe(pipe_chunks[1], &next_copy, sizeof(next_copy));
      atomic_inc64(&copy_queue);
    } else {
      if (!copyFile(src, src_ident, dest, dest_data, pstats)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to copy %s->%s : %d : %s",
                 entry, dest_data, errno, strerror(errno));
        errno = 0;
        result = false;
      }
    }
    pstats->Lookup(SHRINKWRAP_STAT_DATA_FILES)->Inc();
    free(src_ident);
  } else {
    pstats->Lookup(SHRINKWRAP_STAT_DATA_FILES_DEDUPED)->Inc();
    pstats->Lookup(SHRINKWRAP_STAT_DATA_BYTES_DEDUPED)->Xadd(src_st->st_size);
  }

  // The target always exists (either previously existed or just created).
  // This is not done in parallel like copyFile
  if (result && dest->do_link(dest->context_, entry, dest_data)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to link %s->%s : %d : %s", entry,
             dest_data, errno, strerror(errno));
    errno = 0;
    result = false;
  }
  free(dest_data);
  return result;
}

bool handle_dir(struct fs_traversal *src,
                struct cvmfs_attr *src_st,
                struct fs_traversal *dest,
                struct cvmfs_attr *dest_st,
                const char *entry) {
  src_st->st_mode |= S_IWUSR;
  if (dest->do_mkdir(dest->context_, entry, src_st)) {
    if (errno == EEXIST) {
      errno = 0;
      if (dest->set_meta(dest->context_, entry, src_st)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Traversal failed to set_meta %s",
                 entry);
        return false;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Traversal failed to mkdir %s : %d : %s",
               entry, errno, strerror(errno));
      return false;
    }
  }
  return true;
}

void add_dir_for_sync(const char *dir, bool recursive) {
  dirs_.push_back(new RecDir(dir, recursive));
}

// Compares possibly null strings.
// If  1->handle destination
// If -1->handle source
// If  0->handle both
int robust_strcmp(const char *src, const char *dest) {
  if (!src) {
    return 1;
  } else if (!dest) {
    return -1;
  } else {
    return strcmp(src, dest);
  }
}

bool Sync(const char *dir,
          struct fs_traversal *src,
          struct fs_traversal *dest,
          bool recursive,
          perf::Statistics *pstats) {
  bool result = true;
  int cmp = 0;

  char **src_dir = NULL;
  size_t src_len = 0;
  ssize_t src_iter = -1;
  char *src_entry = NULL;
  struct cvmfs_attr *src_st = cvmfs_attr_init();
  list_src_dir(src, dir, &src_dir, &src_len);

  char **dest_dir = NULL;
  size_t dest_len = 0;
  ssize_t dest_iter = -1;
  char *dest_entry = NULL;
  struct cvmfs_attr *dest_st = cvmfs_attr_init();
  dest->list_dir(dest->context_, dir, &dest_dir, &dest_len);
  qsort(dest_dir, dest_len, sizeof(char *), strcmp_list);

  // While either have defined members and there hasn't been a failure
  while (result) {
    if (cmp <= 0) {
      getNext(src, dir, src_dir, &src_entry, &src_iter, &src_st, true, true,
              pstats);
    }
    if (cmp >= 0) {
      getNext(dest, dir, dest_dir, &dest_entry, &dest_iter, &dest_st, false,
              false, pstats);
    } else {
      // A destination entry was added
      pstats->Lookup(SHRINKWRAP_STAT_ENTRIES_DEST)->Inc();
    }

    // All entries in both have been visited
    if (!src_entry && !dest_entry)
      break;

    cmp = robust_strcmp(src_entry, dest_entry);

    if (cmp <= 0) {
      // Compares stats to see if they are equivalent
      if (cmp == 0
          && cvmfs_attr_cmp(src_st, dest_st, dest)
          // Also check internal hardlink consistency in destination file system
          // where applicable:
          && (dest_st->cvm_checksum == NULL
              || dest->is_hash_consistent(dest->context_, dest_st))) {
        if (S_ISDIR(src_st->st_mode) && recursive) {
          add_dir_for_sync(src_entry, recursive);
        }
        continue;
      }
      // If not equal, bring dest up-to-date
      switch (src_st->st_mode & S_IFMT) {
        case S_IFREG:
          if (!handle_file(src, src_st, dest, dest_st, src_entry, pstats))
            result = false;
          break;
        case S_IFDIR:
          if (!handle_dir(src, src_st, dest, dest_st, src_entry))
            result = false;
          if (result && recursive)
            add_dir_for_sync(src_entry, recursive);
          break;
        case S_IFLNK:
          // Should likely copy the source of the symlink target
          if (dest->do_symlink(dest->context_, src_entry, src_st->cvm_symlink,
                               src_st)
              != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr,
                     "Traversal failed to symlink %s->%s : %d : %s", src_entry,
                     src_st->cvm_symlink, errno, strerror(errno));
            result = false;
          }
          break;
        case S_IFIFO:
          // Ignore pipe, but continue
          LogCvmfs(kLogCvmfs, kLogStderr, "Skipping pipe: %s", src_entry);
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Encountered unknown file type '%d' for source file %s",
                   src_st->st_mode & S_IFMT, src_entry);
          result = false;
      }
      /* Dest contains something missing from Src */
    } else {
      switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          if (dest->do_unlink(dest->context_, dest_entry) != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr, "Failed to unlink file %s",
                     dest_entry);
            result = false;
          }
          break;
        case S_IFDIR:
          // This is recursive so that the contents of the directory
          // are removed before trying to remove the directory. If
          // tail recursed, do_rmdir will fail as there are still
          // contents.
          if (!Sync(dest_entry, src, dest, true, pstats)) {
            result = false;
            break;
          }
          if (dest->do_rmdir(dest->context_, dest_entry) != 0) {
            LogCvmfs(kLogCvmfs, kLogStderr, "Failed to remove directory %s",
                     dest_entry);
            result = false;
          }
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Encountered unknown file type '%d' for destination file %s",
                   dest_st->st_mode & S_IFMT, dest_entry);
          result = false;
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

  return result;
}

bool SyncFull(struct fs_traversal *src,
              struct fs_traversal *dest,
              perf::Statistics *pstats,
              uint64_t last_print_time) {
  if (dirs_.empty()) {
    dirs_.push_back(new RecDir("", true));
  }
  while (!dirs_.empty()) {
    RecDir *next_dir = dirs_.back();
    dirs_.pop_back();

    if (!Sync(next_dir->dir, src, dest, next_dir->recursive, pstats)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "File %s failed to copy\n",
               next_dir->dir);
      return false;
    }

    if (stat_update_period_ > 0
        && platform_monotonic_time() - last_print_time > stat_update_period_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s",
               pstats->PrintList(perf::Statistics::kPrintSimple).c_str());
      last_print_time = platform_monotonic_time();
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
  MainWorkerSpecificContext *sc = static_cast<MainWorkerSpecificContext *>(
      data);
  MainWorkerContext *mwc = sc->mwc;
  perf::Counter *files_transferred = mwc->pstats->Lookup(
      SHRINKWRAP_STAT_COUNT_FILE);

  while (1) {
    FileCopy next_copy;
    {
      const MutexLockGuard m(&lock_pipe);
      ReadPipe(pipe_chunks[0], &next_copy, sizeof(next_copy));
    }
    if (next_copy.IsTerminateJob())
      break;

    if (!next_copy.src || !next_copy.dest) {
      continue;
    }
    if (!copyFile(mwc->src_fs, next_copy.src, mwc->dest_fs, next_copy.dest,
                  mwc->pstats)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "File %s failed to copy\n",
               next_copy.src);
    }
    files_transferred->Inc();

    // Noted in FileCopy: This is freed here to prevent the strings from being
    // double freed when a FileCopy goes out of scope here and at WritePipe
    free(next_copy.src);
    free(next_copy.dest);

    atomic_dec64(&copy_queue);
  }
  return NULL;
}

perf::Statistics *GetSyncStatTemplate() {
  perf::Statistics *result = new perf::Statistics();
  result->Register(SHRINKWRAP_STAT_COUNT_FILE,
                   "Number of files from source repository");
  result->Register(SHRINKWRAP_STAT_COUNT_DIR,
                   "Number of directories from source repository");
  result->Register(SHRINKWRAP_STAT_COUNT_SYMLINK,
                   "Number of symlinks from source repository");
  result->Register(SHRINKWRAP_STAT_COUNT_BYTE,
                   "Byte count of projected repository");
  result->Register(SHRINKWRAP_STAT_ENTRIES_SRC,
                   "Number of file system entries processed in the source");
  result->Register(
      SHRINKWRAP_STAT_ENTRIES_DEST,
      "Number of file system entries processed in the destination");
  result->Register(
      SHRINKWRAP_STAT_DATA_FILES,
      "Number of data files transferred from source to destination");
  result->Register(SHRINKWRAP_STAT_DATA_BYTES,
                   "Bytes transferred from source to destination");
  result->Register(SHRINKWRAP_STAT_DATA_FILES_DEDUPED,
                   "Number of files not copied due to deduplication");
  result->Register(SHRINKWRAP_STAT_DATA_BYTES_DEDUPED,
                   "Number of bytes not copied due to deduplication");
  return result;
}

int SyncInit(struct fs_traversal *src,
             struct fs_traversal *dest,
             const char *base,
             const char *spec,
             uint64_t parallel,
             uint64_t stat_period) {
  num_parallel_ = parallel;
  stat_update_period_ = stat_period;

  perf::Statistics *pstats = GetSyncStatTemplate();

  // Initialization
  atomic_init64(&copy_queue);

  pthread_t *workers = NULL;

  struct MainWorkerSpecificContext *specificWorkerContexts = NULL;

  MainWorkerContext *mwc = NULL;

  if (num_parallel_ > 0) {
    workers = reinterpret_cast<pthread_t *>(
        smalloc(sizeof(pthread_t) * num_parallel_));

    specificWorkerContexts = reinterpret_cast<
        struct MainWorkerSpecificContext *>(
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
      const int retval =
          pthread_create(&workers[i], NULL, MainWorker,
                         static_cast<void *>(&(specificWorkerContexts[i])));
      assert(retval == 0);
    }
  }

  if (spec) {
    delete spec_tree_;
    spec_tree_ = SpecTree::Create(spec);
  }

  uint64_t last_print_time = 0;
  add_dir_for_sync(base, recursive);
  const int result = !SyncFull(src, dest, pstats, last_print_time);

  while (atomic_read64(&copy_queue) != 0) {
    if (platform_monotonic_time() - last_print_time > stat_update_period_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s",
               pstats->PrintList(perf::Statistics::kPrintSimple).c_str());
      last_print_time = platform_monotonic_time();
    }

    SafeSleepMs(100);
  }


  if (num_parallel_ > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Stopping %u workers", num_parallel_);
    for (unsigned i = 0; i < num_parallel_; ++i) {
      FileCopy terminate_workers;
      WritePipe(pipe_chunks[1], &terminate_workers, sizeof(terminate_workers));
    }
    for (unsigned i = 0; i < num_parallel_; ++i) {
      const int retval = pthread_join(workers[i], NULL);
      assert(retval == 0);
    }
    ClosePipe(pipe_chunks);
    delete workers;
    delete specificWorkerContexts;
    delete mwc;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "%s",
           pstats->PrintList(perf::Statistics::kPrintHeader).c_str());
  delete pstats;

  delete spec_tree_;

  return result;
}

int GarbageCollect(struct fs_traversal *fs) {
  LogCvmfs(kLogCvmfs, kLogStdout, "Performing garbage collection...");
  const uint64_t start_time = platform_monotonic_time();
  const int retval = fs->garbage_collector(fs->context_);
  const uint64_t end_time = platform_monotonic_time();
  LogCvmfs(kLogCvmfs, kLogStdout, "Garbage collection took %lu seconds.",
           (end_time - start_time));
  return retval;
}

}  // namespace shrinkwrap
