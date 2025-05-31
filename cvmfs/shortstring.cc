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

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
