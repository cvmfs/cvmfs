/**
 * This file is part of the CernVM File System.
 */
#include "data_dir_mgmt.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>

#include <algorithm>
#include <string>

#include "helpers.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "util/posix.h"
#include "util/smalloc.h"

/**
 * Number of directories per level (16^kDigitsPerDirLevel)
 */
static const unsigned kDirsPerLevel = 1U << (4 * kDigitsPerDirLevel);

struct posix_dir_init_thread {
  const char *data;
  mode_t mode;
  unsigned thread_total;
  unsigned thread_num;
};

/**
 * Returns the name of the i-th directory of a level
 * (zero padded hexadecimal, kDigitsPerDirLevel digits)
 */
static std::string DirLevelName(unsigned i) {
  assert(kDigitsPerDirLevel <= 99);
  char dir_name_template[5];
  snprintf(dir_name_template, sizeof(dir_name_template), "%%%02ux",
           kDigitsPerDirLevel);
  char hex[kDigitsPerDirLevel + 1];
  snprintf(hex, sizeof(hex), dir_name_template, i);
  return std::string(hex);
}

static void PosixMkdir(const std::string &path, mode_t mode) {
  const int res = mkdir(path.c_str(), mode);
  assert(res == 0 || errno == EEXIST);
}

/**
 * Method which recursively creates the .data subdirectories below cur_path
 * (which is expected to exist already). depth is the level of the directories
 * created by this call.
 *
 * If the last directory of a level exists we assume that the level was fully
 * created by a previous run and only descend into it. This keeps the cost of
 * re-initializing an existing destination at O(kDirLevels).
 */
static void PosixCheckDirStructure(const std::string &cur_path, mode_t mode,
                                   unsigned depth) {
  if (depth > kDirLevels)
    return;
  const std::string max_dir_name = DirLevelName(kDirsPerLevel - 1);
  if (DirectoryExists(cur_path + "/" + max_dir_name)) {
    // Directories on this level fully created; check ./
    PosixCheckDirStructure(cur_path + "/" + max_dir_name, mode, depth + 1);
    return;
  }
  // Directories in this level not yet fully created...
  for (unsigned i = 0; i < kDirsPerLevel; i++) {
    // Go through directories 0^kDigitsPerDirLevel to f^kDigitsPerDirLevel
    const std::string this_path = cur_path + "/" + DirLevelName(i);
    PosixMkdir(this_path, mode);
    // Once directory created: Prepare substructures
    PosixCheckDirStructure(this_path, mode, depth + 1);
  }
}

/**
 * Main worker of the parallel .data directory initialization.
 * The thread with thread number j creates the top level directories
 * /j/, /j+n/, /j+2n/, ... (where n is the total number of threads) together
 * with all their subdirectories. The subtrees rooted at the top level
 * directories are independent of each other, so no synchronization is needed.
 */
static void *PosixDataDirInitMainWorker(void *data) {
  struct posix_dir_init_thread
      *thread_context = reinterpret_cast<struct posix_dir_init_thread *>(data);
  for (unsigned i = thread_context->thread_num; i < kDirsPerLevel;
       i += thread_context->thread_total) {
    const std::string this_path = std::string(thread_context->data) + "/"
                                  + DirLevelName(i);
    PosixMkdir(this_path, thread_context->mode);
    PosixCheckDirStructure(this_path, thread_context->mode, 2);
  }
  return NULL;
}

/**
 * Creates the .data directory structure.
 * Will use posix_ctx->num_threads in parallel for the task.
 * For posix_ctx->num_threads <= 1 there is a fallback to a sequential version.
 */
void InitializeDataDirectory(struct fs_traversal_context *ctx) {
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          ctx->ctx);
  const mode_t mode = 0700;
  const bool res = MkdirDeep(ctx->data, mode);
  assert(res);
  if (kDirLevels == 0)
    return;

  // More threads than top level directories do not help
  const unsigned thread_total = std::min(
      static_cast<unsigned>(std::max(posix_ctx->num_threads, 1)),
      kDirsPerLevel);

  // The sequential version is also used if the top level was already created
  // by a previous run, in which case there is nothing left to parallelize.
  if (thread_total <= 1
      || DirectoryExists(std::string(ctx->data) + "/"
                         + DirLevelName(kDirsPerLevel - 1))) {
    PosixCheckDirStructure(ctx->data, mode, 1);
    return;
  }

  struct posix_dir_init_thread
      *thread_contexts = reinterpret_cast<struct posix_dir_init_thread *>(
          smalloc(sizeof(struct posix_dir_init_thread) * thread_total));
  pthread_t *workers = reinterpret_cast<pthread_t *>(
      smalloc(sizeof(pthread_t) * thread_total));
  for (unsigned i = 0; i < thread_total; i++) {
    thread_contexts[i].data = ctx->data;
    thread_contexts[i].mode = mode;
    thread_contexts[i].thread_total = thread_total;
    thread_contexts[i].thread_num = i;
    const int retval = pthread_create(&workers[i], NULL, PosixDataDirInitMainWorker,
                                &thread_contexts[i]);
    assert(retval == 0);
  }
  for (unsigned i = 0; i < thread_total; i++) {
    pthread_join(workers[i], NULL);
  }
  free(workers);
  free(thread_contexts);
}
