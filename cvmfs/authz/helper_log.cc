/**
 * This file is part of the CernVM File System.
 */

#include "helper_log.h"

#include <errno.h>
#include <fcntl.h>
#include <syslog.h>
#include <unistd.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

using namespace std;  // NOLINT

namespace {
int fd_debug = -1;
FILE *file_debug = NULL;
int syslog_facility = LOG_USER;
int syslog_level = LOG_NOTICE;
char *syslog_prefix = NULL;
}  // namespace


void SetLogAuthzDebug(const string &path) {
  assert(!path.empty());
  if (fd_debug >= 0)
    close(fd_debug);
  const int fd_debug = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
  if (fd_debug < 0) {
    syslog(LOG_USER | LOG_ERR, "could not open debug log %s (%d), abort",
           path.c_str(), errno);
    abort();
  }
  file_debug = fdopen(fd_debug, "a");
  assert(file_debug != NULL);
}


void SetLogAuthzSyslogLevel(const int level) {
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


void SetLogAuthzSyslogFacility(const int local_facility) {
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


void SetLogAuthzSyslogPrefix(const string &prefix) {
  if (syslog_prefix)
    free(syslog_prefix);

  if (prefix == "") {
    syslog_prefix = NULL;
  } else {
    const unsigned len = prefix.length() + 1;
    syslog_prefix = static_cast<char *>(malloc(len));
    assert(syslog_prefix != NULL);
    syslog_prefix[len - 1] = '\0';
    memcpy(syslog_prefix, &prefix[0], prefix.length());
  }
}


void LogAuthz(const int flags, const char *format, ...) {
  char *msg = NULL;
  va_list variadic_list;
  va_start(variadic_list, format);
  const int retval = vasprintf(&msg, format, variadic_list);
  assert(retval != -1);  // else: out of memory
  va_end(variadic_list);

  if ((flags & kLogAuthzDebug) && (file_debug != NULL)) {
    time_t rawtime;
    time(&rawtime);
    struct tm now;
    localtime_r(&rawtime, &now);
    fprintf(file_debug, "%s    [%02d-%02d-%04d %02d:%02d:%02d %s]\n", msg,
            (now.tm_mon) + 1, now.tm_mday, (now.tm_year) + 1900, now.tm_hour,
            now.tm_min, now.tm_sec, now.tm_zone);
    fflush(file_debug);
  }

  if (flags & (kLogAuthzSyslog | kLogAuthzSyslogWarn | kLogAuthzSyslogErr)) {
    int level = syslog_level;
    if (flags & kLogAuthzSyslogWarn)
      level = LOG_WARNING;
    if (flags & kLogAuthzSyslogErr)
      level = LOG_ERR;
    if (syslog_prefix) {
      syslog(syslog_facility | level, "(%s) %s", syslog_prefix, msg);
    } else {
      syslog(syslog_facility | level, "%s", msg);
    }
  }

  free(msg);
}
