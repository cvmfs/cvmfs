/**
 * This file is part of the CernVM File System.
 *
 * LogCvmfs() handles all message output.  It works like printf.
 * It can log to a debug log file, stdout, stderr, and syslog.
 * The tracer is a separate module, messages do not overlap with logging.
 *
 * The syslog setter routines are not thread-safe.  They are meant to be
 * invoked at the very first, single-threaded stage.
 *
 * If DEBUGMSG is undefined, pure debug messages are compiled into no-ops.
 */

#include "logging_internal.h"

#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <syslog.h>

#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <ctime>
#include <cstring>

#include "smalloc.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace {

pthread_mutex_t lock_stdout = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock_stderr = PTHREAD_MUTEX_INITIALIZER;
#ifdef DEBUGMSG
pthread_mutex_t lock_debug = PTHREAD_MUTEX_INITIALIZER;
FILE *file_debug = NULL;
string *path_debug = NULL;
#endif
const char *module_names[] = { "unknown", "cache", "catalog", "sql", "cvmfs",
  "hash", "download", "compress", "quota", "talk", "monitor", "lru",
  "fuse stub", "signature", "peers", "fs traversal", "catalog traversal",
  "nfs maps", "publish", "spooler", "concurrency", "utility", "glue buffer" };
int syslog_facility = LOG_USER;
int syslog_level = LOG_NOTICE;
char *syslog_prefix = NULL;
LogLevels min_log_level = kLogNormal;
static void (*alt_log_func)(const LogSource source, const int mask,
                            const char *msg) = NULL;

}  // namespace


/**
 * Sets the level that is used for all messages to the syslog facility.
 */
void SetLogSyslogLevel(const int level) {
  switch (level) {
    case 1:
      syslog_level = LOG_DEBUG;
      break;
    case 2:
      syslog_level = LOG_INFO;
      break;
    case 3:
      syslog_level = LOG_NOTICE;
      break;
    default:
      syslog_level = LOG_NOTICE;
      break;
  }
}


int GetLogSyslogLevel() {
  switch (syslog_level) {
    case LOG_DEBUG:
      return 1;
    case LOG_INFO:
      return 2;
    default:
      return 3;
  }
}


/**
 * Sets the syslog facility to one of local0 .. local7.
 * Falls back to LOG_USER if local_facility is not in [0..7]
 */
void SetLogSyslogFacility(const int local_facility) {
  switch (local_facility) {
    case 0:
      syslog_facility = LOG_LOCAL0;
      break;
    case 1:
      syslog_facility = LOG_LOCAL1;
      break;
    case 2:
      syslog_facility = LOG_LOCAL2;
      break;
    case 3:
      syslog_facility = LOG_LOCAL3;
      break;
    case 4:
      syslog_facility = LOG_LOCAL4;
      break;
    case 5:
      syslog_facility = LOG_LOCAL5;
      break;
    case 6:
      syslog_facility = LOG_LOCAL6;
      break;
    case 7:
      syslog_facility = LOG_LOCAL7;
      break;
    default:
      syslog_facility = LOG_USER;
  }
}


int GetLogSyslogFacility() {
  switch (syslog_facility) {
    case LOG_LOCAL0:
      return 0;
    case LOG_LOCAL1:
      return 1;
    case LOG_LOCAL2:
      return 2;
    case LOG_LOCAL3:
      return 3;
    case LOG_LOCAL4:
      return 4;
    case LOG_LOCAL5:
      return 5;
    case LOG_LOCAL6:
      return 6;
    case LOG_LOCAL7:
      return 7;
   default:
      return -1;
  }
}


/**
 * The syslog prefix is used to distinguish multiple repositories in
 * /var/log/messages
 */
void SetLogSyslogPrefix(const std::string &prefix) {
  if (syslog_prefix)
    free(syslog_prefix);

  if (prefix == "") {
    syslog_prefix = NULL;
  } else {
    unsigned len = prefix.length() + 1;
    syslog_prefix = static_cast<char *>(smalloc(len));
    syslog_prefix[len-1] = '\0';
    memcpy(syslog_prefix, &prefix[0], prefix.length());
  }
}


/**
 * Set the minimum verbosity level.  By default kLogNormal.
 */
void SetLogVerbosity(const LogLevels min_level) {
  min_log_level = min_level;
}


/**
 * Changes the debug log file from stderr. No effect if DEBUGMSG is undefined.
 */
#ifdef DEBUGMSG
void SetLogDebugFile(const string &filename) {
  if ((file_debug != NULL) && (file_debug != stderr)) {
    if ((fclose(file_debug) < 0)) {
      fprintf(stderr, "could not close current log file (%d), aborting\n",
              errno);
      abort();
    }
  }
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
  if ((fd < 0) || ((file_debug = fdopen(fd, "a")) == NULL)) {
    fprintf(stderr, "could not open log file %s (%d), aborting\n",
            filename.c_str(), errno);
    abort();
  }
  delete path_debug;
  path_debug = new string(filename);
}


string GetLogDebugFile() {
  if (path_debug)
    return *path_debug;
  return "";
}
#endif


void SetAltLogFunc(void (*fn)(const LogSource source, const int mask,
                              const char *msg))
{
  alt_log_func = fn;
}

/**
 * Logs a message to one or multiple facilities specified by mask.
 * Mask can be extended by a log level in the future, using the higher bits.
 *
 * @param[in] source Component that triggers the logging
 * @param[in] mask Bit mask of log facilities
 * @param[in] format Format string followed by arguments like printf
 */
void LogCvmfs(const LogSource source, const int mask, const char *format, ...) {
  char *msg = NULL;
  va_list variadic_list;

  // Log level check, no flag set in mask means kLogNormal
#ifndef DEBUGMSG
  int log_level = mask & ((2*kLogNone - 1) ^ (kLogLevel0 - 1));
  if (!log_level)
    log_level = kLogNormal;
  if (log_level < min_log_level)
    return;
#endif

  // Format the message string
  va_start(variadic_list, format);
  int retval = vasprintf(&msg, format, variadic_list);
  assert(retval != -1);  // else: out of memory
  va_end(variadic_list);

  if (alt_log_func) {
    (*alt_log_func)(source, mask, msg);
    return;
  }

#ifdef DEBUGMSG
  if (mask & kLogDebug) {
    pthread_mutex_lock(&lock_debug);

    // Set the file pointer for debuging to stderr, if necessary
    if (file_debug == NULL)
      file_debug = stderr;

    //  Get timestamp
    time_t rawtime;
    time(&rawtime);
    struct tm now;
    localtime_r(&rawtime, &now);

    if (file_debug == stderr) pthread_mutex_lock(&lock_stderr);
    fprintf(file_debug, "(%s) %s    [%02d-%02d-%04d %02d:%02d:%02d %s]\n",
            module_names[source], msg,
            (now.tm_mon)+1, now.tm_mday, (now.tm_year)+1900, now.tm_hour,
            now.tm_min, now.tm_sec, now.tm_zone);
    fflush(file_debug);
    if (file_debug == stderr) pthread_mutex_unlock(&lock_stderr);

    pthread_mutex_unlock(&lock_debug);
  }
#endif

  if (mask & kLogStdout) {
    pthread_mutex_lock(&lock_stdout);
    if (mask & kLogShowSource)
      printf("(%s) ", module_names[source]);
    printf("%s", msg);
    if (!(mask & kLogNoLinebreak))
      printf("\n");
    else
      fflush(stdout);
    pthread_mutex_unlock(&lock_stdout);
  }

  if (mask & kLogStderr) {
    pthread_mutex_lock(&lock_stderr);
    if (mask & kLogShowSource)
      printf("(%s) ", module_names[source]);
    fprintf(stderr, "%s", msg);
    if (!(mask & kLogNoLinebreak))
      printf("\n");
    pthread_mutex_unlock(&lock_stderr);
  }

  if (mask & kLogSyslog) {
    if (syslog_prefix) {
      syslog(syslog_facility | syslog_level, "(%s) %s",
             syslog_prefix, msg);
    } else {
      syslog(syslog_facility | syslog_level, "%s", msg);
    }
  }

  free(msg);
}


void PrintError(const string &message) {
  LogCvmfs(kLogCvmfs, kLogStderr, "[ERROR] %s", message.c_str());
}


void PrintWarning(const string &message) {
  LogCvmfs(kLogCvmfs, kLogStderr, "[WARNING] %s", message.c_str());
}

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
