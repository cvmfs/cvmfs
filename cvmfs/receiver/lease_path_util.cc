/**
 * This file is part of the CernVM File System.
 */

#include "lease_path_util.h"

namespace receiver {

bool IsPathInLease(const PathString& lease, const PathString& path) {
  // If lease is "", any path falls within item
  if (!strcmp(lease.c_str(), "")) {
    return true;
  }

  // Is the lease string is the prefix of the path string and the next
  // character of the path string is a "/".
  if (path.StartsWith(lease) && path.GetChars()[lease.GetLength()] == '/') {
    return true;
  }

  // The lease is a prefix of the path and the last char of the lease is a "/"
  if (path.StartsWith(lease) &&
      lease.GetChars()[lease.GetLength() - 1] == '/') {
    return true;
  }

  // If the path string is exactly the lease path return true (allow the
  // creation of the leased directory during the lease itself)
  if (lease == path) {
    return true;
  }

  return false;
}

}  // namespace receiver
