/**
 * This file is part of the CernVM File System.
 */

// Internal use, include only logging.h!

#ifndef CVMFS_UTIL_LOGGING_INTERNAL_H_
#define CVMFS_UTIL_LOGGING_INTERNAL_H_

#include <cstdarg>
#include <ctime>
#include <string>
#include <vector>

#include "util/export.h"

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

/**
 * Default logging facilities
 *
 * Classes which are reused in different parts of CVMFS may need to log to
 * different facilities. For example, in the client logging should be done
 * to the system log, while in cvmfs_swissknife it should be done to stdout.
 *
 * When logging to the default facilities:
 *
 * LogCvmfs(kLogCvmfs, DefaultLogging::info, ...)
 *
 * the facilities can be changed as needed, without modifying the caller code.
 *
 * The default facilities are kLogStdout and kLogStderr.
 */
struct CVMFS_EXPORT DefaultLogging {
  /**
   * Change the default logging facilities
   */
  static void Set(LogFacilities info, LogFacilities error);

  static LogFacilities info;   // default kLogStdout
  static LogFacilities error;  // default kLogStderr
};

enum LogFlags {
  kLogNoLinebreak = 0x200,
  kLogShowSource = 0x400,
  kLogSensitive = 0x800,  ///< Don't add the line to the memory log buffer
};

enum LogLevels {
  kLogLevel0 = 0x01000,
  kLogNormal = 0x02000,
  kLogInform = 0x04000,
  kLogVerbose = 0x08000,
  kLogNone = 0x10000,
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
  kLogReceiver,
  kLogUploadS3,
  kLogUploadGateway,
  kLogS3Fanout,
  kLogGc,
  kLogDns,
  kLogAuthz,
  kLogReflog,
  kLogKvStore,
  kLogTelemetry,
  kLogCurl
};

const int kLogWarning = kLogStdout | kLogShowSource | kLogNormal;
const int kLogInfoMsg = kLogStdout | kLogShowSource | kLogInform;
const int kLogVerboseMsg = kLogStdout | kLogShowSource | kLogVerbose;

struct CVMFS_EXPORT LogBufferEntry {
  LogBufferEntry(LogSource s, int m, const std::string &msg)
      : timestamp(time(NULL)), source(s), mask(m), message(msg) { }

  time_t timestamp;
  LogSource source;
  int mask;
  std::string message;
};

CVMFS_EXPORT void SetLogSyslogLevel(const int level);
CVMFS_EXPORT int GetLogSyslogLevel();
CVMFS_EXPORT void SetLogSyslogFacility(const int facility);
CVMFS_EXPORT int GetLogSyslogFacility();
CVMFS_EXPORT void SetLogCustomFile(unsigned id, const std::string &filename);
CVMFS_EXPORT void SetLogMicroSyslog(const std::string &filename);
CVMFS_EXPORT std::string GetLogMicroSyslog();
CVMFS_EXPORT void SetLogMicroSyslogMaxSize(unsigned bytes);
CVMFS_EXPORT void SetLogSyslogPrefix(const std::string &prefix);
CVMFS_EXPORT void SetLogSyslogShowPID(bool flag);
CVMFS_EXPORT void SetLogVerbosity(const LogLevels max_level);
CVMFS_EXPORT void LogShutdown();

#ifdef DEBUGMSG
CVMFS_EXPORT void SetLogDebugFile(const std::string &filename);
CVMFS_EXPORT std::string GetLogDebugFile();
#else
#define SetLogDebugFile(filename) ((void)0)
#define GetLogDebugFile()         (std::string(""))
#endif

CVMFS_EXPORT
void SetAltLogFunc(void (*fn)(const LogSource source, const int mask,
                              const char *msg));

CVMFS_EXPORT std::vector<LogBufferEntry> GetLogBuffer();
CVMFS_EXPORT void ClearLogBuffer();

CVMFS_EXPORT void PrintWarning(const std::string &message);
CVMFS_EXPORT void PrintError(const std::string &message);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_LOGGING_INTERNAL_H_
