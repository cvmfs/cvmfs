
#include "log.h"

#include <syslog.h>

static int syslog_level = LOG_NOTICE;

void syslog_setlevel(const int level) {
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

void logmsg(const char *msg, ...) {
   va_list vl;
	
   va_start(vl, msg);
   vsyslog(LOG_MAKEPRI(LOG_USER, syslog_level), 
           msg, vl);
   va_end(vl);
}
