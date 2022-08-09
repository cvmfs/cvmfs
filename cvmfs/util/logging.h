/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_LOGGING_H_
#define CVMFS_UTIL_LOGGING_H_

#include <string>

#include "util/export.h"
// Shared declarations of debug and non-debug logging
#include "util/logging_internal.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

CVMFS_EXPORT
void LogCvmfs(const LogSource source, const int mask, const char *format, ...);
// Ensure that pure debug messages are not compiled except in DEBUGMSG mode
#ifndef DEBUGMSG
#define LogCvmfs(source, mask, ...) \
  (((mask) == static_cast<int>(kLogDebug)) ? \
    ((void)0) : LogCvmfs(source, mask, __VA_ARGS__))
#endif

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_LOGGING_H_
