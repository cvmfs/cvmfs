/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_LEASE_PATH_H_
#define CVMFS_RECEIVER_LEASE_PATH_H_

#include "shortstring.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool IsSubPath(const PathString& parent, const PathString& path);

bool PathLeadsToLease(const PathString& lease, const PathString& path);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_RECEIVER_LEASE_PATH_H_
