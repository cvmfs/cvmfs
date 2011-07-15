#ifndef CVMFS_H
#define CVMFS_H 1

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
   int catalog_cache_memusage_bytes();
   void catalog_cache_memusage_slots(int &positive, int &negative, int &all,
                                     int &inserts, int &replaces, int &cleans, int &hits, int &misses,
                                     int &cert_hits, int &cert_misses);
   void info_loaded_catalogs(std::vector<std::string> &prefix, std::vector<time_t> &last_modified, 
                             std::vector<time_t> &expires);
   int clear_file(const std::string &path);
   int remount();
   unsigned get_max_ttl(); /* in minutes */
   void set_max_ttl(const unsigned value); /* in minutes */
}


#endif
