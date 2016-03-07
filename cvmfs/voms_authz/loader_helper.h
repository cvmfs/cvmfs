/**
 * This file is part of the CernVM File System.
 *
 * This contains a few helpful functions for managing dynamic libs.
 */

#ifndef CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_
#define CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_

#include <dlfcn.h>

#include "../logging.h"


bool OpenDynLib(void **handle, const char *name, const char *friendly_name);
void CloseDynLib(void **handle, const char *name);
template<typename T>
bool LoadSymbol(void *lib_handle, T *sym, const char *name) {
  if (!sym) {return false;}
  *sym = reinterpret_cast<typeof(T)>(dlsym(lib_handle, name));
  if (!(*sym)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to load %s: %s", name, dlerror());
  }
  return *sym;
}

#endif  // CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_
