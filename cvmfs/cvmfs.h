#ifndef CVMFS_CVMFS_H_
#define CVMFS_CVMFS_H_

#include <string>
#include <vector>
#include "tracer.h"
#include <unistd.h>
#include <time.h>

namespace cvmfs {

   extern pid_t pid;
   extern std::string root_url;
   extern std::string mountpoint;
   extern int max_cache_timeout;
//   int catalog_cache_memusage_bytes();
//   void catalog_cache_memusage_slots(int *positive, int *negative, int *all,
//                                     int *inserts, int *replaces, int *cleans, int *hits, int *misses,
//                                     int *cert_hits, int *cert_misses);
   int clear_file(const std::string &path);
   int remount();
   unsigned get_max_ttl(); /* in minutes */
   void set_max_ttl(const unsigned value); /* in minutes */
}


#endif // CVMFS_CVMFS_H_
