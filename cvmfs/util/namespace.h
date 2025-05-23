/**
 * This file is part of the CernVM File System.
 *
 * Wrappers around the unshare() call. Implementation on Linux only, on macOS
 * implementation is empty and returns with error codes.
 */

#ifndef CVMFS_UTIL_NAMESPACE_H_
#define CVMFS_UTIL_NAMESPACE_H_

#include <unistd.h>

#include <string>

#include "util/export.h"

const int kNsFeatureMount = 0x01;
const int kNsFeaturePid = 0x02;
const int kNsFeatureUserAvailable = 0x04;
const int kNsFeatureUserEnabled = 0x08;

enum NamespaceFailures {
  kFailNsOk = 0,
  kFailNsUnsuppored,
  kFailNsUnshare,
  kFailNsMapUidOpen,
  kFailNsMapUidWrite,
  kFailNsSetgroupsOpen,
  kFailNsSetgroupsWrite,
  kFailNsMapGidOpen,
  kFailNsMapGidWrite,
};

CVMFS_EXPORT int CheckNamespaceFeatures();

CVMFS_EXPORT
NamespaceFailures CreateUserNamespace(uid_t map_uid_to, gid_t map_gid_to);
CVMFS_EXPORT bool CreateMountNamespace();
CVMFS_EXPORT bool CreatePidNamespace(int *fd_parent);

CVMFS_EXPORT bool BindMount(const std::string &from, const std::string &to);
CVMFS_EXPORT bool ProcMount(const std::string &to);

#endif  // CVMFS_UTIL_NAMESPACE_H_
