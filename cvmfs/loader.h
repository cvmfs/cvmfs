/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_H_
#define CVMFS_LOADER_H_

#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64

#include <stdint.h>
#include <fuse/fuse_lowlevel.h>

#include <cstring>
#include <string>

namespace loader {

enum Failures {
  kFailOk = 0,
  kFailOptions,
  kFailPermission,
  kFailMount,
  kFailFuseLoop,
  kFailLoadLibrary,
  lFailIncompatibleVersions,  // TODO
  kFailCacheDir,
  kFailPeers,
  kFailNfsMaps,
  kFailQuota,
  kFailMonitor,
  kFailTalk,
  kFailSignature,
  kFailCatalog,
  kFailUnknown,
};


// TODO SaveState


/**
 * This contains the public interface of the cvmfs loader.
 * Whenever something changes, change the version number.
 */
struct LoaderExports {
  LoaderExports() {
    version = 1;
    size = sizeof(LoaderExports);
    foreground = false;
  }

  uint32_t version;
  uint32_t size;
  bool foreground;
  std::string repository_name;
  std::string mount_point;
  std::string config_files;
  std::string program_name;
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
    fnInit = NULL;
    fnSpawn = NULL;
    fnFini = NULL;
    fnGetErrorMsg = NULL;
    memset(&cvmfs_operations, 0, sizeof(cvmfs_operations));
  }

  uint32_t version;
  uint32_t size;

  int (*fnInit)(const LoaderExports *loader_exports);
  void (*fnSpawn)();
  void (*fnFini)();
  std::string (*fnGetErrorMsg)();
  struct fuse_lowlevel_ops cvmfs_operations;
};

}  // namespace loader

#endif  // CVMFS_LOADER_H_
