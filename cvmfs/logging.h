/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOGGING_H_
#define CVMFS_LOGGING_H_

// Shared declarations of debug and non-debug logging
#include "logging-internal.h"

void LogCvmfs(const LogSource source, const int mask, const char *format, ...);
// Ensure that pure debug messages are not compiled excpet in DEBUGMSG mode
#ifndef DEBUGMSG
#define LogCvmfs(source, mask, ...) \
  (((mask) == kLogDebug) ? ((void)0) : LogCvmfs(source, mask, __VA_ARGS__))
#endif

#endif  // CVMFS_LOGGING_H_
