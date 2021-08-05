/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_H_
#define CVMFS_LOADER_H_

#define _FILE_OFFSET_BITS 64

#include <stdint.h>
#include <time.h>

#include <cstring>
#include <string>
#include <vector>

#include "duplex_fuse.h"

namespace loader {

extern std::string *usyslog_path_;

/**
 * Possible failures when booting/mounting cvmfs.  Remember to add a constant
 * to libcvmfs.h and libcvmfs_legacy.cc when a constant to this enum is added.
 */
enum Failures {
  kFailOk = 0,
  kFailUnknown,
  kFailOptions,
  kFailPermission,
  kFailMount,
  kFailLoaderTalk,
  kFailFuseLoop,
  kFailLoadLibrary,
  kFailIncompatibleVersions,  // TODO(jblomer)
  kFailCacheDir,
  kFailPeers,
  kFailNfsMaps,
  kFailQuota,
  kFailMonitor,
  kFailTalk,
  kFailSignature,
  kFailCatalog,
  kFailMaintenanceMode,
  kFailSaveState,
  kFailRestoreState,
  kFailOtherMount,
  kFailDoubleMount,
  kFailHistory,
  kFailWpad,
  kFailLockWorkspace,
  kFailRevisionBlacklisted,

  kFailNumEntries
};

inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "unknown error";
  texts[2] = "illegal options";
  texts[3] = "permission denied";
  texts[4] = "failed to mount";
  texts[5] = "unable to init loader talk socket";
  texts[6] = "cannot run FUSE event loop";
  texts[7] = "failed to load shared library";
  texts[8] = "incompatible library version";
  texts[9] = "cache directory/plugin problem";
  texts[10] = "peering problem";
  texts[11] = "NFS maps init failure";
  texts[12] = "quota init failure";
  texts[13] = "watchdog failure";
  texts[14] = "talk socket failure";
  texts[15] = "signature verification failure";
  texts[16] = "file catalog failure";
  texts[17] = "maintenance mode";
  texts[18] = "state saving failure";
  texts[19] = "state restore failure";
  texts[20] = "already mounted";
  texts[21] = "double mount";
  texts[22] = "history init failure";
  texts[23] = "proxy auto-discovery failed";
  texts[24] = "workspace already locked";
  texts[25] = "revision blacklisted";
  texts[26] = "no text";
  return texts[error];
}


enum StateId {
  kStateUnknown = 0,
  kStateOpenDirs,           // >= 2.1.4
  kStateOpenChunks,         // >= 2.1.4, used as of 2.1.15
  kStateGlueBuffer,         // >= 2.1.9
  kStateInodeGeneration,    // >= 2.1.9
  kStateOpenFilesCounter,   // >= 2.1.9
  kStateGlueBufferV2,       // >= 2.1.10
  kStateGlueBufferV3,       // >= 2.1.15
  kStateGlueBufferV4,       // >= 2.1.20
  kStateOpenChunksV2,       // >= 2.1.20
  kStateOpenChunksV3,       // >= 2.2.0
  kStateOpenChunksV4,       // >= 2.2.3
  kStateOpenFiles,          // >= 2.4
  kStateNentryTracker       // >= 2.7

  // Note: kStateOpenFilesXXX was renamed to kStateOpenChunksXXX as of 2.4
};


struct SavedState {
  SavedState() {
    version = 1;
    size = sizeof(SavedState);
    state_id = kStateUnknown;
    state = NULL;
  }

  uint32_t version;
  uint32_t size;
  StateId state_id;
  void *state;
};
typedef std::vector<SavedState *> StateList;


struct LoadEvent {
  LoadEvent() {
    version = 1;
    size = sizeof(LoadEvent);
    timestamp = 0;
  }

  uint32_t version;
  uint32_t size;
  time_t timestamp;
  std::string so_version;
};
typedef std::vector<LoadEvent *> EventList;


/**
 * This contains the public interface of the cvmfs loader.
 * Whenever something changes, change the version number.
 *
 * Note: Do not forget to check the version of LoaderExports in cvmfs.cc when
 *       using fields that were not present in version 1
 *
 * CernVM-FS 2.1.8 --> Version 2
 * CernVM-FS 2.2.0 --> Version 3
 * CernVM-FS 2.4.0 --> Version 4
 * CernVM-FS 2.7.0 --> Version 4, fuse_channel --> fuse_channel_or_session
 * CernVM-FS 2.9.0 --> Version 5, add device_id
 */
struct LoaderExports {
  LoaderExports() :
    version(5),
    size(sizeof(LoaderExports)),
    boot_time(0),
    foreground(false),
    disable_watchdog(false),
    simple_options_parsing(false),
    fuse_channel_or_session(NULL)
  { }

  ~LoaderExports() {
    for (unsigned i = 0; i < history.size(); ++i)
      delete history[i];
  }

  uint32_t version;
  uint32_t size;
  time_t boot_time;
  std::string loader_version;
  bool foreground;
  std::string repository_name;
  std::string mount_point;
  std::string config_files;
  std::string program_name;
  EventList history;
  StateList saved_states;

  // added with CernVM-FS 2.1.8 (LoaderExports Version: 2)
  bool disable_watchdog;

  // added with CernVM-FS 2.2.0 (LoaderExports Version: 3)
  bool simple_options_parsing;

  // added with CernVM-FS 2.4.0 (LoaderExports Version: 4)
  // As of CernVM-FS 2.7, this has been rebranded from
  // struct fuse_chan **fuse_channel  to
  // void **fuse_channel_or_session
  // in order to work with both libfuse2 and libfuse3
  void **fuse_channel_or_session;

  // Linux only, stores the major:minor internal mountpoint identifier
  // The identifier is read just after mount from /proc/self/mountinfo
  // If it cannot be determined (e.g. on macOS), device_id is "0:0".
  std::string device_id;
};


/**
 * This contains the public interface of the cvmfs fuse module.
 * Whenever something changes, change the version number.
 * A global CvmfsExports struct is looked up by the loader via dlsym.
 *
 * Note: as of cvmfs version 2.8, we set cvmfs_operations.forget_multi on new
 * enough fuse
 */
struct CvmfsExports {
  CvmfsExports() {
    version = 1;
    size = sizeof(CvmfsExports);
    fnAltProcessFlavor = NULL;
    fnInit = NULL;
    fnSpawn = NULL;
    fnFini = NULL;
    fnGetErrorMsg = NULL;
    fnMaintenanceMode = NULL;
    fnSaveState = NULL;
    fnRestoreState = NULL;
    fnFreeSavedState = NULL;
    memset(&cvmfs_operations, 0, sizeof(cvmfs_operations));
  }

  uint32_t version;
  uint32_t size;
  std::string so_version;

  int (*fnAltProcessFlavor)(int argc, char **argv);
  int (*fnInit)(const LoaderExports *loader_exports);
  void (*fnSpawn)();
  void (*fnFini)();
  std::string (*fnGetErrorMsg)();
  bool (*fnMaintenanceMode)(const int fd_progress);
  bool (*fnSaveState)(const int fd_progress, StateList *saved_states);
  bool (*fnRestoreState)(const int fd_progress, const StateList &saved_states);
  void (*fnFreeSavedState)(const int fd_progress,
                           const StateList &saved_states);
  struct fuse_lowlevel_ops cvmfs_operations;
};

Failures Reload(const int fd_progress, const bool stop_and_go);

}  // namespace loader

#endif  // CVMFS_LOADER_H_
