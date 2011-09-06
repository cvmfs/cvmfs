#ifdef _BUILT_IN_LIBFUSE
  #include "fuse_lowlevel.h"
  #include "fuse_opt.h"
#else
  #include <fuse_lowlevel.h>
  #include <fuse/fuse_opt.h>
#endif
