/**
 * This file is part of the CernVM File System.
 *
 * This contains a few helpful functions for managing dynamic libs.
 */

#ifndef CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_
#define CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_

bool OpenDynLib(void **handle, const char *name, const char *friendly_name);
void CloseDynLib(void **handle, const char *name);
template<typename T> bool LoadSymbol(void *lib_handle, T *sym,
                                     const char *name);

#endif  // CVMFS_VOMS_AUTHZ_LOADER_HELPER_H_
