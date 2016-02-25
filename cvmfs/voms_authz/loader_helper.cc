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


template<typename T>
bool LoadSymbol(void *lib_handle, T *sym, const char *name) {
  if (!sym) {return false;}
  *sym = reinterpret_cast<typeof(T)>(dlsym(lib_handle, name));
  if (!(*sym)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to load %s: %s", name, dlerror());
  }
  return *sym;
}

struct vomsdata;
struct x509_st;
struct stack_st_X509;
template bool LoadSymbol<vomsdata* (*)(char*, char*)>(
                        void*, vomsdata* (**)(char*, char*), char const*);
template bool LoadSymbol<void (*)(vomsdata*)>(void*, void (**)(vomsdata*),
                        char const*);
template bool LoadSymbol<int (*)(x509_st*, stack_st_X509*, int, vomsdata*,
                         int*)>
                        (void*,
                         int (**)(x509_st*, stack_st_X509*, int, vomsdata*,
                                 int*),
                         char const*);
template bool LoadSymbol<char* (*)(vomsdata*, int, char*, int)>(void*,
                        char* (**)(vomsdata*, int, char*, int), char const*);
template bool LoadSymbol<int (*)(char**, int*, vomsdata*, int*)>(void*,
                        int (**)(char**, int*, vomsdata*, int*), char const*);
template bool LoadSymbol<int (*)(char*, int, vomsdata*, int*)>(void*,
                        int (**)(char*, int, vomsdata*, int*), char const*);
