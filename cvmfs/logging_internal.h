/**
 * This file is part of the CernVM File System.
 */

// Internal use, include only logging.h!

#ifndef CVMFS_LOGGING_INTERNAL_H_
#define CVMFS_LOGGING_INTERNAL_H_

#include <cstdarg>
#include <string>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

enum LogFacilities {
  kLogDebug = 1,
  kLogStdout = 2,
  kLogStderr = 4,
  kLogSyslog = 8,
  kLogSyslogWarn = 16,
  kLogSyslogErr = 32,
};

enum LogFlags {
  kLogNoLinebreak = 128,
  kLogShowSource = 256,
};

enum LogLevels {
  kLogLevel0 = 1024,
  kLogVerbose = 2048,
  kLogNormal = 4096,
  kLogDiscrete = 8192,
  kLogNone = 16384,
};

/**
 * Changes in this enum must be done in logging.cc as well!
 * (see const char *module_names[] = {....})
 */
enum LogSource {
  kLogCache = 1,
  kLogCatalog,
  kLogSql,
  kLogCvmfs,
  kLogHash,
  kLogDownload,
  kLogCompress,
  kLogQuota,
  kLogTalk,
  kLogMonitor,
  kLogLru,
  kLogFuse,
  kLogSignature,
  kLogFsTraversal,
  kLogCatalogTraversal,
  kLogNfsMaps,
  kLogPublish,
  kLogSpooler,
  kLogConcurrency,
  kLogUtility,
  kLogGlueBuffer,
  kLogHistory,
  kLogUnionFs,
  kLogPathspec,
  kLogUploadS3,
  kLogS3Fanout,
  kLogGc,
  kLogDns,
  kLogVoms
};

const int kLogVerboseMsg = kLogStdout | kLogShowSource | kLogVerbose;
const int kLogWarning    = kLogStdout | kLogShowSource | kLogNormal;

void SetLogSyslogLevel(const int level);
int GetLogSyslogLevel();
void SetLogSyslogFacility(const int facility);
int GetLogSyslogFacility();
void SetLogMicroSyslog(const std::string &filename);
std::string GetLogMicroSyslog();
void SetLogSyslogPrefix(const std::string &prefix);
void SetLogVerbosity(const LogLevels min_level);

#ifdef DEBUGMSG
void SetLogDebugFile(const std::string &filename);
std::string GetLogDebugFile();
#else
#define SetLogDebugFile(filename) ((void)0)
#define GetLogDebugFile() (std::string(""))
#endif

void SetAltLogFunc(void (*fn)(const LogSource source, const int mask,
                              const char *msg));

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_LOGGING_INTERNAL_H_
