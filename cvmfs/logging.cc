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

#include "logging_internal.h"  // NOLINT(build/include)

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "platform.h"
#include "smalloc.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace {

const unsigned kMicroSyslogMax = 500*1024;  // rotate after 500kB

pthread_mutex_t lock_stdout = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock_stderr = PTHREAD_MUTEX_INITIALIZER;
#ifdef DEBUGMSG
pthread_mutex_t lock_debug = PTHREAD_MUTEX_INITIALIZER;
FILE *file_debug = NULL;
string *path_debug = NULL;
#endif
const char *module_names[] = { "unknown", "cache", "catalog", "sql", "cvmfs",
  "hash", "download", "compress", "quota", "talk", "monitor", "lru",
  "fuse stub", "signature", "fs traversal", "catalog traversal",
  "nfs maps", "publish", "spooler", "concurrency", "utility", "glue buffer",
  "history", "unionfs", "pathspec", "upload s3", "s3fanout", "gc", "dns",
  "voms" };
int syslog_facility = LOG_USER;
int syslog_level = LOG_NOTICE;
char *syslog_prefix = NULL;

string *usyslog_dest = NULL;
int usyslog_fd = -1;
int usyslog_fd1 = -1;
unsigned usyslog_size = 0;
pthread_mutex_t lock_usyslock = PTHREAD_MUTEX_INITIALIZER;

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
 * "Micro-Syslog" write kLogSyslog messages into filename.  It rotates this
 * file.  Requires for µCernVM
 */
void SetLogMicroSyslog(const std::string &filename) {
  pthread_mutex_lock(&lock_usyslock);
  if (usyslog_fd >= 0) {
    close(usyslog_fd);
    close(usyslog_fd1);
    usyslog_fd = -1;
    usyslog_fd1 = -1;
  }

  if (filename == "") {
    delete usyslog_dest;
    usyslog_dest = NULL;
    pthread_mutex_unlock(&lock_usyslock);
    return;
  }

  usyslog_fd = open(filename.c_str(), O_RDWR | O_APPEND | O_CREAT, 0600);
  if (usyslog_fd < 0) {
    fprintf(stderr, "could not open usyslog file %s (%d), aborting\n",
            filename.c_str(), errno);
    abort();
  }
  usyslog_fd1 = open((filename + ".1").c_str(), O_WRONLY | O_CREAT, 0600);
  if (usyslog_fd1 < 0) {
    fprintf(stderr, "could not open usyslog.1 file %s.1 (%d), aborting\n",
            filename.c_str(), errno);
    abort();
  }
  platform_stat64 info;
  int retval = platform_fstat(usyslog_fd, &info);
  assert(retval == 0);
  usyslog_size = info.st_size;
  usyslog_dest = new string(filename);
  pthread_mutex_unlock(&lock_usyslock);
}


std::string GetLogMicroSyslog() {
  pthread_mutex_lock(&lock_usyslock);
  string result;
  if (usyslog_dest)
    result = *usyslog_dest;
  pthread_mutex_unlock(&lock_usyslock);
  return result;
}


static void LogMicroSyslog(const std::string &message) {
  if (message.size() == 0)
    return;

  pthread_mutex_lock(&lock_usyslock);
  if (usyslog_fd < 0) {
    pthread_mutex_unlock(&lock_usyslock);
    return;
  }

  int written = write(usyslog_fd, message.data(), message.size());
  if ((written < 0) || (static_cast<unsigned>(written) != message.size())) {
    close(usyslog_fd);
    usyslog_fd = -1;
    abort();
  }
  int retval = fsync(usyslog_fd);
  assert(retval == 0);
  usyslog_size += written;

  if (usyslog_size >= kMicroSyslogMax) {
    // Wipe out usyslog.1 file
    retval = ftruncate(usyslog_fd1, 0);
    assert(retval == 0);

    // Copy from usyslog to usyslog.1
    retval = lseek(usyslog_fd, 0, SEEK_SET);
    assert(retval == 0);
    unsigned char buf[4096];
    int num_bytes;
    do {
      num_bytes = read(usyslog_fd, buf, 4096);
      assert(num_bytes >= 0);
      if (num_bytes == 0)
        break;
      int written = write(usyslog_fd1, buf, num_bytes);
      assert(written == num_bytes);
    } while (num_bytes == 4096);
    retval = lseek(usyslog_fd1, 0, SEEK_SET);
    assert(retval == 0);

    // Reset usyslog
    retval = lseek(usyslog_fd, 0, SEEK_SET);
    assert(retval == 0);
    retval = ftruncate(usyslog_fd, 0);
    assert(retval == 0);
    usyslog_size = 0;
  }
  pthread_mutex_unlock(&lock_usyslock);
}


/**
 * Changes the debug log file from stderr. No effect if DEBUGMSG is undefined.
 */
#ifdef DEBUGMSG
void SetLogDebugFile(const string &filename) {
  if (filename == "") {
    if ((file_debug != NULL) && (file_debug != stderr)) {
      fclose(file_debug);
      file_debug = NULL;
    }
    delete path_debug;
    path_debug = NULL;
    return;
  }

  if ((file_debug != NULL) && (file_debug != stderr)) {
    if ((fclose(file_debug) < 0)) {
      fprintf(stderr, "could not close current log file (%d), aborting\n",
              errno);

      abort();
    }
  }
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
  if ((fd < 0) || ((file_debug = fdopen(fd, "a")) == NULL)) {
    fprintf(stderr, "could not open debug log file %s (%d), aborting\n",
            filename.c_str(), errno);
    syslog(syslog_facility | LOG_ERR, "could not open debug log file %s (%d), "
           "aborting\n", filename.c_str(), errno);
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

    // Get timestamp
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
    fflush(stdout);
    pthread_mutex_unlock(&lock_stdout);
  }

  if (mask & kLogStderr) {
    pthread_mutex_lock(&lock_stderr);
    if (mask & kLogShowSource)
      fprintf(stderr, "(%s) ", module_names[source]);
    fprintf(stderr, "%s", msg);
    if (!(mask & kLogNoLinebreak))
      fprintf(stderr, "\n");
    fflush(stderr);
    pthread_mutex_unlock(&lock_stderr);
  }

  if (mask & (kLogSyslog | kLogSyslogWarn | kLogSyslogErr)) {
    if (usyslog_dest) {
      string fmt_msg(msg);
      if (syslog_prefix)
        fmt_msg = "(" + string(syslog_prefix) + ") " + fmt_msg;
      time_t rawtime;
      time(&rawtime);
      char fmt_time[26];
      ctime_r(&rawtime, fmt_time);
      fmt_msg = string(fmt_time, 24) + " " + fmt_msg;
      fmt_msg.push_back('\n');
      LogMicroSyslog(fmt_msg);
    } else {
      int level = syslog_level;
      if (mask & kLogSyslogWarn) level = LOG_WARNING;
      if (mask & kLogSyslogErr) level = LOG_ERR;
      if (syslog_prefix) {
        syslog(syslog_facility | level, "(%s) %s",
               syslog_prefix, msg);
      } else {
        syslog(syslog_facility | level, "%s", msg);
      }
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
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
