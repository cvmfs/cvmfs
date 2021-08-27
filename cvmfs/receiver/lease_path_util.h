/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_LEASE_PATH_UTIL_H_
#define CVMFS_RECEIVER_LEASE_PATH_UTIL_H_

#include "shortstring.h"

namespace receiver {

bool IsSubPath(const PathString& parent, const PathString& path);

}

#endif  // CVMFS_RECEIVER_LEASE_PATH_UTIL_H_
