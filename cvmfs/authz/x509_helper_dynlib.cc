/**
 * This file is part of the CernVM File System.
 */
#include "x509_helper_dynlib.h"

#include "authz/x509_helper_log.h"

bool
OpenDynLib(void **handle, const char *name, const char *friendly_name) {
  if (!handle) {return false;}
  *handle = dlopen(name, RTLD_LAZY);
  if (!(*handle)) {
    LogAuthz(kLogAuthzDebug | kLogAuthzSyslogErr, "Failed to load %s library; "
             "authz will not be available.  %s", friendly_name, dlerror());
  }
  return *handle;
}


void
CloseDynLib(void **handle, const char *name) {
  if (!handle) {return;}
  if (*handle && dlclose(*handle)) {
    LogAuthz(kLogAuthzDebug | kLogAuthzSyslogErr,
             "Failed to unload %s library: %s", name, dlerror());
  }
  *handle = NULL;
}
