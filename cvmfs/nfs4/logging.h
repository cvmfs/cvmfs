/**
 * This file is part of the CernVM File System.
 *
 * Logging for the NFS server.  Kept separate from cvmfs' util logging so that
 * the libfuse replacement library, which stays loaded across cvmfs reloads,
 * does not pin libcvmfs_util.
 */

#ifndef CVMFS_NFS4_LOGGING_H_
#define CVMFS_NFS4_LOGGING_H_

namespace nfs4 {

enum LogLevel {
  kLogLevelDebug = 0,
  kLogLevelInfo,
  kLogLevelWarn,
  kLogLevelError,
};

typedef void (*LogSink)(LogLevel level, const char *message);

/**
 * The default sink sends info and above to syslog, and everything to stderr
 * if CVMFS_NFS4_DEBUG is set in the environment.
 */
void SetLogSink(LogSink sink);

void Log(LogLevel level, const char *format, ...)
    __attribute__((format(printf, 2, 3)));

}  // namespace nfs4

#endif  // CVMFS_NFS4_LOGGING_H_
