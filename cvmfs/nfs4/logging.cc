/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/logging.h"

#include <sys/time.h>
#include <syslog.h>

#include <cstdarg>
#include <cstdio>
#include <cstdlib>

namespace nfs4 {

namespace {

FILE *DebugStream() {
  const char *debug = getenv("CVMFS_NFS4_DEBUG");
  if (debug == NULL)
    return NULL;
  // A path redirects the trace, e.g. for a daemon without stderr
  if (debug[0] == '/') {
    FILE *f = fopen(debug, "a");
    if (f) {
      setvbuf(f, NULL, _IOLBF, 0);
      return f;
    }
  }
  return stderr;
}

void DefaultSink(LogLevel level, const char *message) {
  static FILE *debug = DebugStream();
  if (debug) {
    struct timeval now;
    gettimeofday(&now, NULL);
    fprintf(debug, "%ld.%03d (nfs4) %s\n", static_cast<long>(now.tv_sec),  // NOLINT
            static_cast<int>(now.tv_usec / 1000), message);
  }
  switch (level) {
    case kLogLevelInfo:
      syslog(LOG_INFO, "(nfs4) %s", message);
      break;
    case kLogLevelWarn:
      syslog(LOG_WARNING, "(nfs4) %s", message);
      break;
    case kLogLevelError:
      syslog(LOG_ERR, "(nfs4) %s", message);
      break;
    default:
      break;
  }
}

LogSink g_sink = DefaultSink;

}  // anonymous namespace

void SetLogSink(LogSink sink) { g_sink = sink ? sink : DefaultSink; }

void Log(LogLevel level, const char *format, ...) {
  char buf[1024];
  va_list args;
  va_start(args, format);
  vsnprintf(buf, sizeof(buf), format, args);
  va_end(args);
  g_sink(level, buf);
}

}  // namespace nfs4
