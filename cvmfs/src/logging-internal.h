/**
 * This file is part of the CernVM File System.
 */

// Internal use, include only logging.h!

#ifndef CVMFS_LOGGING_INTERNAL_H_
#define CVMFS_LOGGING_INTERNAL_H_

#include <cstdarg>
#include <string>

enum LogFacilities {
  kLogDebug = 1,
  kLogStdout = 2,
  kLogStderr = 4,
  kLogSyslog = 8,
};

enum LogSource {
  kLogCache = 1,
  kLogCatalog,
  kLogCvmfs,
  kLogHash,
  kLogDownload,
  kLogCompress,
  kLogLru,
  kLogTalk,
  kLogMonitor,
  kLogInodeCache,
  kLogPathCache,
  kLogMd5Cache,
};

void SetLogSyslogLevel(const int level);
void SetLogSyslogPrefix(const std::string &prefix);

#ifdef DEBUGMSG
void SetLogDebugFile(const std::string &filename);
#else
#define SetLogDebugFile(filename) ((void)0)
#endif

#endif  // CVMFS_LOGGING_INTERNAL_H_
