/**
 * This file is part of the CernVM File System.
 */

#include "lease_path_util.h"

namespace receiver {

bool IsPathInLease(const PathString& lease, const PathString& path) {

  // If lease is "", then any path is a subpath
  if (lease.GetLength() == 0) {
    return true;
  }

  // If the lease string is the prefix of the path string and either
  // the strings are identical or the separator character is a "/",
  // then the path is a subpath
  if (path.StartsWith(lease) &&
      ((path.GetLength() == lease.GetLength()) ||
       (path.GetChars()[lease.GetLength()] == '/') ||
       (path.GetChars()[lease.GetLength() - 1] == '/'))) {
    return true;
  }

  return false;
}

}  // namespace receiver
