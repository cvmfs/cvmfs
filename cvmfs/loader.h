/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_H_
#define CVMFS_LOADER_H_

#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64

#include <stdint.h>
#include <time.h>
#include <fuse/fuse_lowlevel.h>

#include <cstring>
#include <string>
#include <vector>

namespace loader {

enum Failures {
  kFailOk = 0,
  kFailUnknown,
  kFailOptions,
  kFailPermission,
  kFailMount,
  kFailLoaderTalk,
  kFailFuseLoop,
  kFailLoadLibrary,
  kFailIncompatibleVersions,  // TODO
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
};


enum StateId {
  kStateUnknown = 0,
  kStateOpenDirs,
  kStateOpenFiles,
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
 */
struct LoaderExports {
  LoaderExports() {
    version = 1;
    size = sizeof(LoaderExports);
    foreground = false;
    boot_time = 0;
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
};


/**
 * This contains the public interface of the cvmfs fuse module.
 * Whenever something changes, change the version number.
 * A global CvmfsExports struct is looked up by the loader via dlsym.
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
    fnFreeSavedStates = NULL;
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
  void (*fnFreeSavedStates)(const int fd_progress,
                            const StateList &saved_states);
  struct fuse_lowlevel_ops cvmfs_operations;
};

Failures Reload(const int fd_progress);

}  // namespace loader

#endif  // CVMFS_LOADER_H_
