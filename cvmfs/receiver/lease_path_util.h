/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_LEASE_PATH_UTIL_H_
#define CVMFS_RECEIVER_LEASE_PATH_UTIL_H_

#include "shortstring.h"

namespace receiver {

bool IsPathInLease(const PathString& lease, const PathString& path);

}

#endif  // CVMFS_RECEIVER_LEASE_PATH_UTIL_H_
