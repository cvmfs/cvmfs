/**
 * This file is part of the CernVM File System.
 */
#include "garbage_collector.h"

#include <dirent.h>
#include <errno.h>

#include <cstdio>
#include <cstring>
#include <map>
#include <string>

#include "helpers.h"
#include "shrinkwrap/fs_traversal.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

/**
 * Initializes garbage collection by setting up the corresponding hash map
 * in the fs_traversal_posix_context
 * The initialization uses the data stored in the file at
 * [data directory] + POSIX_GARBAGE_DIR + POSIX_GARBAGE_FLAGGED_FILE
 *
 * This file contains a fixed length concatenation of
 * inodes (type ino_t) which should be checked
 * and deleted upon garbage collection
 */
void InitializeGarbageCollection(struct fs_traversal_context *ctx) {
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          ctx->ctx);
  int res2 = mkdir((std::string(ctx->data) + POSIX_GARBAGE_DIR).c_str(), 0700);
  assert(res2 == 0 || errno == EEXIST);
  std::string gc_path = std::string(ctx->data) + POSIX_GARBAGE_DIR
                        + POSIX_GARBAGE_FLAGGED_FILE;
  if (FileExists(gc_path)) {
    FILE *gc_flagged_file = fopen(gc_path.c_str(), "r");
    assert(gc_flagged_file != NULL);
    while (true) {
      ino_t cur_ino;
      size_t read = fread(&cur_ino, sizeof(ino_t), 1, gc_flagged_file);
      if (read == 1) {
        posix_ctx->gc_flagged[cur_ino] = true;
      } else {
        assert(feof(gc_flagged_file) != 0);
        break;
      }
    }
    int res = fclose(gc_flagged_file);
    assert(res == 0);
  }
}

/**
 * Finalizes the garbage collection
 * Stores the content of the garbage collection hash map in the file at
 * [data directory] + POSIX_GARBAGE_DIR + POSIX_GARBAGE_FLAGGED_FILE
 */
void FinalizeGarbageCollection(struct fs_traversal_context *ctx) {
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          ctx->ctx);
  std::string gc_path = std::string(ctx->data) + POSIX_GARBAGE_DIR
                        + POSIX_GARBAGE_FLAGGED_FILE;
  FILE *gc_flagged_file = fopen(gc_path.c_str(), "w");
  for (std::map<ino_t, bool>::const_iterator it = posix_ctx->gc_flagged.begin();
       it != posix_ctx->gc_flagged.end();
       it++) {
    if (it->second) {
      fwrite(&(it->first), sizeof(ino_t), 1, gc_flagged_file);
    }
  }
  fclose(gc_flagged_file);
}

/**
 * Main Worker of the Posix Garbage Collector
 * Iterates over a calculated set of directories and check for inodes that can
 * be deleted.
 * For the thread with thread number j the thread starts with the top level data
 * directory /j/ and iterates over the contained files. Afterwards it continues
 * with /j+i*n/ where n is the number of total threads and i is an arbitrary
 * integer.
 */
void *PosixGcMainWorker(void *data) {
  struct posix_gc_thread
      *thread_context = reinterpret_cast<struct posix_gc_thread *>(data);
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          thread_context->ctx->ctx);
  int64_t files_removed = 0;
  int64_t bytes_removed = 0;
  // Build path array
  int offset = strlen(thread_context->ctx->data) + 1;
  // used for both path building and stat calls (therefore +257)
  char dir_path[offset + kDigitsPerDirLevel * kDirLevels + kDirLevels + 257];
  snprintf(dir_path, offset, "%s", thread_context->ctx->data);
  dir_path[offset - 1] = '/';
  dir_path[offset + kDigitsPerDirLevel * kDirLevels + kDirLevels + 257] = '\0';
  char dir_name_template[6];
  snprintf(dir_name_template,
           sizeof(dir_name_template),
           "%%%02ux/",
           kDigitsPerDirLevel);
  const unsigned directory_mask = (1 << (kDigitsPerDirLevel * 4)) - 1;
  const unsigned max_dir_name = (1 << kDigitsPerDirLevel * 4);
  const unsigned max_val = (1 << (kDirLevels * kDigitsPerDirLevel * 4));
  for (unsigned i =
           thread_context->thread_num * max_dir_name * (kDirLevels - 1);
       i < max_val;
       i += thread_context->thread_total * +(max_val / max_dir_name)) {
    // Iterate over paths of current subdirectory...
    for (unsigned j = i; j < i + (max_val / max_dir_name); j++) {
      // For every subdirectory chain (described by j)
      unsigned path_pos = offset;
      for (int level = kDirLevels - 1; level >= 0; level--) {
        const unsigned cur_dir = (j >> (level * kDigitsPerDirLevel * 4))
                                 & directory_mask;
        snprintf(dir_path + path_pos, kDigitsPerDirLevel + 2, dir_name_template,
                 cur_dir);
        path_pos += kDigitsPerDirLevel + 1;
      }
      dir_path[path_pos] = '\0';
      // Calculated path - now garbage collection...
      DIR *cur_dir_ent = opendir(dir_path);
      assert(cur_dir_ent != NULL);
      struct stat stat_buf;
      struct dirent *de;
      while ((de = readdir(cur_dir_ent)) != NULL) {
        if (posix_ctx->gc_flagged.count(de->d_ino) > 0
            && posix_ctx->gc_flagged[de->d_ino]) {
          snprintf(dir_path + path_pos, sizeof(dir_path) - path_pos, "%s",
                   de->d_name);
          stat(dir_path, &stat_buf);
          if (stat_buf.st_nlink == 1) {
            files_removed++;
            bytes_removed += stat_buf.st_size;
            int res = unlink(dir_path);
            assert(res == 0);
            posix_ctx->gc_flagged.erase(de->d_ino);
          }
        }
      }
      closedir(cur_dir_ent);
    }
  }
  thread_context->stat->Lookup(POSIX_GC_STAT_FILES_REMOVED)
      ->Xadd(files_removed);
  thread_context->stat->Lookup(POSIX_GC_STAT_BYTES_REMOVED)
      ->Xadd(bytes_removed);
  return NULL;
}

/**
 * Method which runs the garbage collection.
 * Will start posix_ctx->num_threads in parallel for the task.
 * For posix_ctx->num_threads <= 1 there is a fallback to a sequential version.
 */
int RunGarbageCollection(struct fs_traversal_context *ctx) {
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          ctx->ctx);

  int thread_total = posix_ctx->num_threads;
  struct posix_gc_thread
      *thread_contexts = reinterpret_cast<struct posix_gc_thread *>(
          smalloc(sizeof(struct posix_gc_thread) * thread_total));

  perf::Statistics *gc_statistics = new perf::Statistics();
  gc_statistics->Register(
      POSIX_GC_STAT_FILES_REMOVED,
      "Number of deduplicated files removed by Garbage Collector");
  gc_statistics->Register(POSIX_GC_STAT_BYTES_REMOVED,
                          "Sum of sizes of removed files");

  if (thread_total > 1) {
    pthread_t *workers = reinterpret_cast<pthread_t *>(
        smalloc(sizeof(pthread_t) * thread_total));
    for (int i = 0; i < thread_total; i++) {
      thread_contexts[i].thread_total = thread_total;
      thread_contexts[i].thread_num = i;
      thread_contexts[i].ctx = ctx;
      thread_contexts[i].stat = gc_statistics;
      int retval = pthread_create(&workers[i], NULL, PosixGcMainWorker,
                                  &thread_contexts[i]);
      assert(retval == 0);
    }

    for (int i = 0; i < thread_total; i++) {
      pthread_join(workers[i], NULL);
    }
    free(workers);
  } else {
    thread_contexts[0].thread_total = thread_total;
    thread_contexts[0].thread_num = 0;
    thread_contexts[0].ctx = ctx;
    thread_contexts[0].stat = gc_statistics;
    PosixGcMainWorker(thread_contexts);
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "%s",
           gc_statistics->PrintList(perf::Statistics::kPrintHeader).c_str());
  free(thread_contexts);
  delete gc_statistics;
  return 0;
}
