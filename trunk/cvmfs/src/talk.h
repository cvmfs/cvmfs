#ifndef CVMFS_TALK_H
#define CVMFS_TALK_H 1

#include <string>
#include "tracer.h"

namespace talk {

   bool init(std::string cachedir);
   void spawn();
   void fini();

}

#endif

