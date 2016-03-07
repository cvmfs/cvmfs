/**
 * This file is part of the CernVM File System.
 *
 * This contains a few helpful functions for managing dynamic libs.
 */

#include <dlfcn.h>

#include "../logging.h"
#include "loader_helper.h"


bool
OpenDynLib(void **handle, const char *name, const char *friendly_name) {
  if (!handle) {return false;}
  *handle = dlopen(name, RTLD_LAZY);
  if (!(*handle)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to load %s library; "
             "authz will not be available.  %s", friendly_name, dlerror());
  }
  return *handle;
}


void
CloseDynLib(void **handle, const char *name) {
  if (!handle) {return;}
  if (*handle && dlclose(*handle)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to unload %s library: %s",
                                  name, dlerror());
  }
  *handle = NULL;
}
