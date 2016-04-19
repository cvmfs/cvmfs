/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_DYNLIB_H_
#define CVMFS_AUTHZ_X509_HELPER_DYNLIB_H_

#include <dlfcn.h>

#include "authz/x509_helper_log.h"


bool OpenDynLib(void **handle, const char *name, const char *friendly_name);
void CloseDynLib(void **handle, const char *name);
template<typename T>
bool LoadSymbol(void *lib_handle, T *sym, const char *name) {
  if (!sym) {return false;}
  *sym = reinterpret_cast<typeof(T)>(dlsym(lib_handle, name));
  if (!(*sym)) {
    LogAuthz(kLogAuthzDebug, "Failed to load %s: %s", name, dlerror());
  }
  return *sym;
}

#endif  // CVMFS_AUTHZ_X509_HELPER_DYNLIB_H_
