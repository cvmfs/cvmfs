/**
 * This file is part of the CernVM File System.
 */

#include "util/lease_path.h"


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool IsSubPath(const PathString& parent, const PathString& path) {
  // If parent is "", then any path is a subpath
  if (parent.GetLength() == 0) {
    return true;
  }

  // If the parent string is the prefix of the path string and either
  // the strings are identical or the separator character is a "/",
  // then the path is a subpath
  if (path.StartsWith(parent) &&
      ((path.GetLength() == parent.GetLength()) ||
       (path.GetChars()[parent.GetLength()] == '/') ||
       (path.GetChars()[parent.GetLength() - 1] == '/'))) {
    return true;
  }

  return false;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
