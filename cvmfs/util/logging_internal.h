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
struct DefaultLogging {
  /**
   * Change the default logging facilities
   */
  static void Set(LogFacilities info, LogFacilities error);

  static LogFacilities info;  // default kLogStdout
  static LogFacilities error;  // default kLogStderr
};

enum LogFlags {
  kLogNoLinebreak = 0x200,
  kLogShowSource  = 0x400,
  kLogSensitive   = 0x800,  ///< Don't add the line to the memory log buffer
};

enum LogLevels {
  kLogLevel0   = 0x01000,
  kLogNormal   = 0x02000,
  kLogInform   = 0x04000,
  kLogVerbose  = 0x08000,
  kLogNone     = 0x10000,
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
};

const int kLogWarning = kLogStdout | kLogShowSource | kLogNormal;
const int kLogInfoMsg = kLogStdout | kLogShowSource | kLogInform;
const int kLogVerboseMsg = kLogStdout | kLogShowSource | kLogVerbose;

struct LogBufferEntry {
  LogBufferEntry(LogSource s, int m, const std::string &msg)
    : timestamp(time(NULL)), source(s), mask(m), message(msg) { }

  time_t timestamp;
  LogSource source;
  int mask;
  std::string message;
};

void SetLogSyslogLevel(const int level);
int GetLogSyslogLevel();
void SetLogSyslogFacility(const int facility);
int GetLogSyslogFacility();
void SetLogCustomFile(unsigned id, const std::string &filename);
void SetLogMicroSyslog(const std::string &filename);
std::string GetLogMicroSyslog();
void SetLogMicroSyslogMaxSize(unsigned bytes);
void SetLogSyslogPrefix(const std::string &prefix);
void SetLogSyslogShowPID(bool flag);
void SetLogVerbosity(const LogLevels max_level);
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

std::vector<LogBufferEntry> GetLogBuffer();
void ClearLogBuffer();

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_LOGGING_INTERNAL_H_
