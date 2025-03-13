/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */


#include "shortstring.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

PathString GetParentPath(const PathString &path) {
  const int length = static_cast<int>(path.GetLength());
  if (length == 0)
    return path;
  const char *chars = path.GetChars();

  for (int i = length - 1; i >= 0; --i) {
    if (chars[i] == '/')
      return PathString(chars, i);
  }

  return path;
}

NameString GetFileName(const PathString &path) {
  NameString name;
  const int length = static_cast<int>(path.GetLength());
  const char *chars = path.GetChars();

  int i;
  for (i = length - 1; i >= 0; --i) {
    if (chars[i] == '/')
      break;
  }
  i++;
  if (i < length) {
    name.Append(chars + i, length - i);
  }

  return name;
}


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
