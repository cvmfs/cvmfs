#ifndef LOG_H
#define LOG_H 1

#include <stdarg.h>

void syslog_setlevel(const int level);
void logmsg(const char *msg, ...);

#endif
