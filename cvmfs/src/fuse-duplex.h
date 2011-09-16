#ifndef FUSE_DUPLEX_H
#define FUSE_DUPLEX_H 1

#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64

#ifdef _BUILT_IN_LIBFUSE
  #include "fuse_lowlevel.h"
  #include "fuse_opt.h"
#else
  #include <fuse_lowlevel.h>
  #include <fuse/fuse_opt.h>
#endif

#endif
