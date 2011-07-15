#ifndef CVMFS_MONITOR_H
#define CVMFS_MONITOR_H 1

#include <string>

namespace monitor {

   bool init(const std::string cache_dir, const bool check_nofiles);
   void fini();
   void spawn();
   
   unsigned get_nofiles();

}

#endif
