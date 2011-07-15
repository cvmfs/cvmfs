#ifndef COMPAT_H
#define COMPAT_H

/* -------------------------------------------- 
 *
 *  dummy functions... TODO!!
 *
 * -------------------------------------------- */

inline void fuse_set_cache_drainout() {}
inline void fuse_unset_cache_drainout() {}
inline int fuse_get_cache_drainout() { return 0; }
inline int fuse_get_max_cache_timeout() { return 60; }

#ifdef __APPLE__
	#include "compat_macosx.h"
#else
	#include "compat_linux.h"
#endif

#endif