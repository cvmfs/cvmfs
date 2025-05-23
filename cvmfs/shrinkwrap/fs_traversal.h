/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SHRINKWRAP_FS_TRAVERSAL_H_
#define CVMFS_SHRINKWRAP_FS_TRAVERSAL_H_

#include <string>

#include "fs_traversal.h"
#include "fs_traversal_interface.h"
#include "statistics.h"


#define SHRINKWRAP_STAT_COUNT_FILE         "cntFile"
#define SHRINKWRAP_STAT_COUNT_DIR          "cntDir"
#define SHRINKWRAP_STAT_COUNT_SYMLINK      "cntSymlink"
#define SHRINKWRAP_STAT_COUNT_BYTE         "cntByte"
#define SHRINKWRAP_STAT_ENTRIES_SRC        "entriesSrc"
#define SHRINKWRAP_STAT_ENTRIES_DEST       "entriesDest"
#define SHRINKWRAP_STAT_DATA_FILES         "dataFiles"
#define SHRINKWRAP_STAT_DATA_BYTES         "dataBytes"
#define SHRINKWRAP_STAT_DATA_FILES_DEDUPED "dataFilesDeduped"
#define SHRINKWRAP_STAT_DATA_BYTES_DEDUPED "dataBytesDeduped"

namespace shrinkwrap {

struct fs_traversal *FindInterface(const char *type);

int SyncInit(struct fs_traversal *src,
             struct fs_traversal *dest,
             const char *base,
             const char *spec,
             uint64_t parallel,
             uint64_t stat_period);

int GarbageCollect(struct fs_traversal *fs);

// Public for testing
bool SyncFull(struct fs_traversal *src,
              struct fs_traversal *dest,
              perf::Statistics *pstats,
              uint64_t last_print_time);

// Exported for testing purposes:
perf::Statistics *GetSyncStatTemplate();

}  // namespace shrinkwrap

#endif  // CVMFS_SHRINKWRAP_FS_TRAVERSAL_H_
