
#include "log.h"
#include "smalloc.h"

#include <syslog.h>
#include <string.h>
#include <stdlib.h>

static int syslog_level = LOG_NOTICE;
static char *log_prefix = NULL;

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

void syslog_setprefix(const char *prefix) {
   if (log_prefix)
      free(log_prefix);
   
   int len = strlen(prefix) + 4;
   log_prefix = smalloc(len);
   log_prefix[len-1] = '\0';
   log_prefix[0] = '(';
   memcpy(log_prefix+1, prefix, strlen(prefix));
   log_prefix[strlen(prefix)+1] = ')';
   log_prefix[strlen(prefix)+2] = ' ';
}

void logmsg(const char *msg, ...) {
   va_list vl;
   
   char *tagged_msg;
   
   if (log_prefix) {
      int len = strlen(log_prefix) + strlen(msg) + 1;
      tagged_msg = alloca(len);
      tagged_msg[len-1] = '\0';
      memcpy(tagged_msg, log_prefix, strlen(log_prefix));
      memcpy(tagged_msg+strlen(log_prefix), msg, strlen(msg));
   } else {
      int len = strlen(msg)+1;
      tagged_msg = alloca(len);
      tagged_msg[len-1] = '\0';
      memcpy(tagged_msg, msg, strlen(msg));
   }
	
   va_start(vl, msg);
   vsyslog(LOG_MAKEPRI(LOG_USER, syslog_level), 
           tagged_msg, vl);
   va_end(vl);
}
