/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_H_
#define CVMFS_LOADER_H_

#include <stdint.h>

namespace loader {

enum Failures {
  kFailOk = 0,
  kFailOptions,
};


/**
 * This contains the public interface of the cvmfs fuse module.
 * Whenever something changes, change the version number.
 * A global CvmfsExports struct is looked up by the loader via dlsym.
 */
struct CvmfsExports {
  uint32_t version;
  uint32_t size;
};

}  // namespace loader

#endif  // CVMFS_LOADER_H_
