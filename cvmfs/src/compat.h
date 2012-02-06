#ifndef CVMFS_COMPAT_H_
#define CVMFS_COMPAT_H_

#ifdef __APPLE__
  #include "compat_macosx.h"
#else
  #include "compat_linux.h"
#endif

#endif  // CVMFS_COMPAT_H_
