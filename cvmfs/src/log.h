#ifndef LOG_H
#define LOG_H 1

#include <stdarg.h>

void syslog_setlevel(const int level);
void syslog_setprefix(const char *prefix);
void logmsg(const char *msg, ...);

#endif
