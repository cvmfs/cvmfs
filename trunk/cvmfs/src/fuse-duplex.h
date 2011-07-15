#ifdef _BUILT_IN_LIBFUSE
  #include "fuse.h"
  #include "fuse_opt.h"
#else
  #include <fuse.h>
  #include <fuse/fuse_opt.h>
#endif
