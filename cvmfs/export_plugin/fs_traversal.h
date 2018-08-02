/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_

#include <string>

#include "fs_traversal.h"
#include "fs_traversal_interface.h"
#include "statistics.h"

#define SHRINKWRAP_STAT_BYTE_COUNT "byteCnt"
#define SHRINKWRAP_STAT_FILE_COUNT "fileCnt"
#define SHRINKWRAP_STAT_SRC_ENTRIES "srcEntries"
#define SHRINKWRAP_STAT_DEST_ENTRIES "destEntries"
#define SHRINKWRAP_STAT_DEDUPED_FILES "dedupedFiles"
#define SHRINKWRAP_STAT_DEDUPED_BYTES "dedupedBytes"

namespace shrinkwrap {

struct fs_traversal* FindInterface(const char * type);

int SyncInit(struct fs_traversal *src,
             struct fs_traversal *dest,
             const char *base,
             const char *spec,
             unsigned parallel,
             unsigned retries,
             bool fsck);

int GarbageCollect(struct fs_traversal *fs);

// Public for testing
bool SyncFull(
  struct fs_traversal *src,
  struct fs_traversal *dest,
  perf::Statistics *pstats,
  bool do_fsck);

// Exported for testing purposes:
perf::Statistics *GetSyncStatTemplate();

}  // namespace shrinkwrap

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
