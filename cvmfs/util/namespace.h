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

const int kNsFeatureMount         = 0x01;
const int kNsFeaturePid           = 0x02;
const int kNsFeatureUserAvailable = 0x04;
const int kNsFeatureUserEnabled   = 0x08;

int CheckNamespaceFeatures();

bool CreateUserNamespace(uid_t map_uid_to, gid_t map_gid_to);
bool CreateMountNamespace();
bool CreatePidNamespace(int *fd_parent);

bool BindMount(const std::string &from, const std::string &to);

#endif  // CVMFS_UTIL_NAMESPACE_H_
