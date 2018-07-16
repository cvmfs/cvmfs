/**
 * This file is part of the CernVM File System.
 */

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

unsigned             num_parallel = 1;
bool                 recursive = true;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
// unsigned             retries = 1;
atomic_int64         overall_copies;
atomic_int64         overall_new;
atomic_int64         copy_queue;

}  // anonymous namespace

struct fs_traversal* FindInterface(const char * type)
{
  if (!strcmp(type, "posix")) {
    return posix_get_interface();
  } else if (!strcmp(type, "cvmfs")) {
    return libcvmfs_get_interface();
  }
  return NULL;
}

bool copy_file(
  struct fs_traversal *src_fs,
  const char *src_data,
  struct fs_traversal *dest_fs,
  const char *dest_data,
  int parallel);

bool cvmfs_attr_cmp(struct cvmfs_attr *src, struct cvmfs_attr *dest) {
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

  // CVMFS related content
  if (src->cvm_checksum && dest->cvm_checksum) {
    if (strcmp(src->cvm_checksum, dest->cvm_checksum)) { return false; }
  } else if ((!src->cvm_checksum && dest->cvm_checksum) ||
            (src->cvm_checksum && !dest->cvm_checksum)) {
    return false;
  }

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
  struct cvmfs_attr **st
) {
  cvmfs_attr_free(*st);
  *st = NULL;
  *st = cvmfs_attr_init();
  int retval = fs->get_stat(fs->context_, entry, *st);
  return retval;
}

bool CommandExport::Traverse(
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
    updateStat(src, src_entry, &src_st);
  }

  char **dest_dir = NULL;
  size_t dest_len = 0;
  size_t dest_iter = 0;
  char * dest_entry = NULL;
  struct cvmfs_attr *dest_st = cvmfs_attr_init();
  dest->list_dir(dest->context_, dir, &dest_dir, &dest_len);
  qsort(dest_dir, dest_len, sizeof(char *),  strcmp_list);

  if (getNext(dest, dir, dest_dir, &dest_entry, NULL)) {
    updateStat(dest, dest_entry, &dest_st);
  }


  // While both have defined members to compare.
  while ((src_entry) || (dest_entry)) {
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
      if (!cvmfs_attr_cmp(src_st, dest_st)) {
        // If not equal, bring dest up-to-date
        switch (src_st->st_mode & S_IFMT) {
          case S_IFREG:
            {
              // They don't point to the same data, link new data
              const char *dest_data;
              dest_data = dest->get_identifier(dest->context_, dest_st);
              const char *src_data;
              src_data = src->get_identifier(src->context_, src_st);

              // Touch is atomic, if it fails something else will write file?
              if (!dest->touch(dest->context_, dest_st)) {
                // PUSH TO PIPE
                if (!copy_file(src, src_data, dest, dest_data, parallel)) {
                  LogCvmfs(kLogCvmfs, kLogDebug,
                  "Traversal failed to copy %s->%s", src_data, dest_data);
                  return false;
                }
              }

              // Should probably happen in the copy function as it is parallel
              // Also this needs to be separate from copy_file, the target file
              // could already exist and the link needs to be created anyway.
              if (dest->do_link(dest->context_, dest_entry, dest_data)) {
                  LogCvmfs(kLogCvmfs, kLogDebug,
                  "Traversal failed to link %s->%s", dest_entry, dest_data);
                return false;
              }
            }
            break;
          case S_IFDIR:
            if (dest->set_meta(dest->context_, src_entry, src_st)) {
              LogCvmfs(kLogCvmfs, kLogDebug,
              "Traversal failed to set_meta %s", src_entry);
              return false;
            }
            if (recursive) {
              if (!Traverse(src_entry, src, dest, parallel, recursive)) {
                return false;
              }
            }
            break;
          case S_IFLNK:
            // Should likely copy the source of the symlink target
            if (dest->do_symlink(dest->context_, src_entry,
                             src_st->cvm_symlink, src_st)) {
              LogCvmfs(kLogCvmfs, kLogDebug,
              "Traversal failed to symlink %s->%s",
              src_entry, src_st->cvm_symlink);
              return retval;
            }
            break;
        }
      } else {
        // The directory exists and is the same,
        // but we still need to check the children
        if (S_ISDIR(src_st->st_mode) && recursive) {
          if (!Traverse(src_entry, src, dest, parallel, recursive)) {
            return false;
          }
        }
      }
      if (getNext(src, dir, src_dir, &src_entry, &src_iter)) {
        updateStat(src, src_entry, &src_st);
      }
      if (getNext(dest, dir, dest_dir, &dest_entry, &dest_iter)) {
        updateStat(dest, dest_entry, &dest_st);
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
                  LogCvmfs(kLogCvmfs, kLogDebug,
                  "Traversal failed to copy %s->%s", src_data, dest_data);
                  return false;
                }
              }

              // Should probably happen in the copy function as it is parallel
              // Also this needs to be separate from copy_file, the target file
              // could already exist and the link needs to be created anyway.
              if (dest->do_link(dest->context_, src_entry, dest_data)) {
                LogCvmfs(kLogCvmfs, kLogDebug,
                "Traversal failed to link %s->%s", src_entry, dest_data);
                return false;
              }
            }
          break;
        case S_IFDIR:
          if (dest->do_mkdir(dest->context_, src_entry, src_st)) {
              LogCvmfs(kLogCvmfs, kLogDebug,
              "Traversal failed to mkdir %s", src_entry);
            return false;
          }
          if (recursive) {
            if (!Traverse(src_entry, src, dest, parallel, recursive)) {
              return false;
            }
          }
          break;
        case S_IFLNK:
          // Should be same as IFREG? Does link create the file?
          if (dest->do_symlink(dest->context_, src_entry,
                           src_st->cvm_symlink, src_st)) {
            LogCvmfs(kLogCvmfs, kLogDebug,
            "Traversal failed to symlink %s->%s",
            src_entry, src_st->cvm_symlink);
            return false;
          }
          break;
        default:
          // Unknown file type, should print error (what stream? log?)
          return false;
          break;
      }
      if (getNext(src, dir, src_dir, &src_entry, &src_iter)) {
        updateStat(src, src_entry, &src_st);
      }
    /* Dest contains something missing from Src */
    } else if (cmp > 0) {
      switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          if (dest->do_unlink(dest->context_, dest_entry)) {
            return false;
          }
          break;
        case S_IFDIR:
          // We may want this to be recursive regardless
          if (recursive) {
            if (!Traverse(dest_entry, src, dest, parallel, recursive)) {
              return false;
            }
          }
          if (!dest->do_rmdir(dest->context_, dest_entry)) {
            return false;
          }
          break;
        default:
          // Unknown file type, should print error (what stream? log?)
          return false;
          break;
      }
      if (getNext(dest, dir, dest_dir, &dest_entry, &dest_iter)) {
        updateStat(dest, dest_entry, &dest_st);
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
    printf("Failed open src : %s\n", src_name);
    return false;
  }

  retval = dest_fs->do_fopen(dest, fs_open_write);
  if (retval != 0) {
    // Handle error
    printf("Failed open dest : %s\n", dest_name);
    return false;
  }

  while (1) {
    char buffer[COPY_BUFFER_SIZE];

    size_t actual_read = 0;
    retval = src_fs->do_fread(src, buffer, COPY_BUFFER_SIZE, &actual_read);
    printf("Read : %"PRIu64"\n", actual_read);
    if (retval != 0) {
      return false;
    }

    retval = dest_fs->do_fwrite(dest, buffer, actual_read);
    if (retval != 0) {
      return false;
    }
    printf("Write : %"PRIu64"\n", actual_read);

    if (actual_read < COPY_BUFFER_SIZE) {
      break;
    }
  }

  retval = src_fs->do_fclose(src);
  if (retval != 0) {
    // Handle error
    printf("Failed close src : %s\n", src_name);
    return false;
  }

  retval = dest_fs->do_fclose(dest);
  if (retval != 0) {
    // Handle error
    printf("Failed close dest : %s\n", dest_name);
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
      printf("File %s failed to copy\n\n", next_copy.src);
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

// int CommandExport::Main(const swissknife::ArgumentList &args) {
int CommandExport::Main() {
  // The starting location for the traversal in src
  // Default value is the base directory (only used if not trace provided)
  string base = "";

  string src_repo;
  string src_data;
  string src_type;

  string dest_repo;
  string dest_data;
  string dest_type;

  string trace_file;
/*
  // Option parsing
  if (args.find('b') != args.end())
    base = *args.find('b')->second;

  if (args.find('s') != args.end())
    src_repo = *args.find('s')->second;
  if (args.find('i') != args.end())
    src_data = *args.find('i')->second;
  if (args.find('t') != args.end())
    src_type = *args.find('t')->second;

  if (args.find('d') != args.end())
    dest_repo = *args.find('d')->second;
  if (args.find('o') != args.end())
    dest_data = *args.find('o')->second;
  if (args.find('w') != args.end())
    dest_type = *args.find('w')->second;

  if (args.find('n') != args.end())
    num_parallel = String2Uint64(*args.find('n')->second);
  if (args.find('r') != args.end())
    retries = String2Uint64(*args.find('r')->second);
*/
  struct fs_traversal *src = FindInterface(src_type.c_str());
  src->context_ = src->initialize(src_repo.c_str(), src_data.c_str());

  struct fs_traversal *dest = FindInterface(dest_type.c_str());
  dest->context_ = dest->initialize(dest_repo.c_str(), dest_data.c_str());

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

  if (!trace_file.empty()) {
    ifstream trace(trace_file.c_str());
    std::string entry;
    while (getline(trace, entry))
    {
      // Function removes special characters and determines if its recursive
      recursive = trim_trace_spec(&entry);
      char *entry_point = strdup(entry.c_str());
      result = Traverse(entry_point, src, dest, num_parallel, recursive);
      free(entry_point);
    }
  } else {
    char *entry_point = strdup(base.c_str());
    result = Traverse(entry_point, src, dest, num_parallel, true);
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
