/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOGGING_H_
#define CVMFS_LOGGING_H_

#include <string>

// Shared declarations of debug and non-debug logging
#include "logging_internal.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

void LogCvmfs(const LogSource source, const int mask, const char *format, ...);
// Ensure that pure debug messages are not compiled except in DEBUGMSG mode
#ifndef DEBUGMSG
#define LogCvmfs(source, mask, ...) \
  (((mask) == kLogDebug) ? ((void)0) : LogCvmfs(source, mask, __VA_ARGS__))
#endif

void PrintWarning(const std::string &message);
void PrintError(const std::string &message);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_LOGGING_H_
