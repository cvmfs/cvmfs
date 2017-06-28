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
  kLogDebug = 0x01,
  kLogStdout = 0x02,
  kLogStderr = 0x04,
  kLogSyslog = 0x08,
  kLogSyslogWarn = 0x10,
  kLogSyslogErr = 0x20,
  kLogCustom0 = 0x40,
  kLogCustom1 = 0x80,
  kLogCustom2 = 0x100,
};

enum LogFlags {
  kLogNoLinebreak = 0x200,
  kLogShowSource = 0x400,
};

enum LogLevels {
  kLogLevel0 = 0x800,
  kLogVerbose = 0x1000,
  kLogNormal = 0x2000,
  kLogDiscrete = 0x4000,
  kLogNone = 0x8000,
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
  kLogUploadGateway,
  kLogS3Fanout,
  kLogGc,
  kLogDns,
  kLogAuthz,
  kLogReflog,
  kLogKvStore,
};

const int kLogVerboseMsg = kLogStdout | kLogShowSource | kLogVerbose;
const int kLogWarning = kLogStdout | kLogShowSource | kLogNormal;

void SetLogSyslogLevel(const int level);
int GetLogSyslogLevel();
void SetLogSyslogFacility(const int facility);
int GetLogSyslogFacility();
void SetLogCustomFile(unsigned id, const std::string &filename);
void SetLogMicroSyslog(const std::string &filename);
std::string GetLogMicroSyslog();
void SetLogSyslogPrefix(const std::string &prefix);
void SetLogVerbosity(const LogLevels min_level);
void LogShutdown();

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
