/**
 * ToDo: document
 */

#include "debug.h"
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

unsigned verbosity_mask = 0xffffffff;
char *pmesg_cat[] = { "unknown", "mark", "cache", "catalog", "cvmfs", "hash", "prefetch",
                      "curl", "scvmfs", "compress", "lru", "crowd", "talk", "monitor", "memcached" };
pthread_mutex_t pmesg_lock = PTHREAD_MUTEX_INITIALIZER;

static FILE *file_dbg = NULL;

/**
 * Change the log file.  If the old one cannot be closed or if 
 * the new one cannot be opened, we die;
 * we never close stdout or stderr.
 */  
void debug_set_log(const char *filename) {
   pthread_mutex_lock(&pmesg_lock);

   if ((file_dbg != NULL) && (file_dbg != stderr) && (file_dbg != stdout)) {
      if ((fclose(file_dbg) < 0)) {
         fprintf(stderr, "could not close current log file (%d), aborting\n", errno);
         abort();
      }
   }
   int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, 0600);
   if ((fd < 0) || ((file_dbg = fdopen(fd, "a")) == NULL)) {
      fprintf(stderr, "could not open log file %s (%d), aborting\n", filename, errno);
      abort();
   }

   pthread_mutex_unlock(&pmesg_lock);
}


#ifndef NDEBUGMSG
void pmesg(int mask, const char* format, ...) {
   va_list vl;

   if (mask & verbosity_mask) {
      pthread_mutex_lock(&pmesg_lock);
      
      /* Set the file pointer for debuging to stderr, if necessary */
      if (file_dbg == NULL) {
         file_dbg = stderr;
      }
      
      fprintf(file_dbg, "(");
      int i;
      for (i = 0; i < D_BITMAX; ++i) {
         if (mask & (1 << i)) {
            fprintf(file_dbg, " %s", pmesg_cat[i+1]);
            mask &= ~(1 << i);
         }
      }
      if (mask) fprintf(file_dbg, " %s", pmesg_cat[0]);
      fprintf(file_dbg, " ) ");
      
      va_start(vl, format);
      vfprintf(file_dbg, format, vl);
      va_end(vl);
      
      /* Timestamp and final line break */
      time_t rawtime;
      time(&rawtime);
      struct tm now;
      localtime_r(&rawtime, &now);
      fprintf(file_dbg, "    [%02d-%02d-%04d %02d:%02d:%02d %s]\n",
         (now.tm_mon)+1, now.tm_mday, (now.tm_year)+1900, now.tm_hour, now.tm_min, now.tm_sec, now.tm_zone);
      
      fflush(file_dbg);

      pthread_mutex_unlock(&pmesg_lock);
   }
}
#endif
