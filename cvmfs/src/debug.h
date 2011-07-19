/** 
 * ToDo: document
 */

#ifndef DEBUG_H
#define DEBUG_H

#include <stdarg.h>

#define  D_MARK      1
#define  D_CACHE     2
#define  D_CATALOG   4
#define  D_CVMFS     8
#define  D_HASH      16
#define  D_PREFETCH  32
#define  D_CURL      64
#define  D_SCVMFS    128
#define  D_COMPRESS  256
#define  D_LRU       512
#define  D_CROWD     1024
#define  D_TALK      2048
#define  D_MONIT     4096
#define  D_MEMCACHED 8192
#define  D_BITMAX    14

extern char *pmesg_cat[];
extern unsigned verbosity_mask;

void debug_set_log(const char *filename);

#ifdef NDEBUGMSG
#define pmesg(mask, format, args...) ((void)0)
#else
void pmesg(int mask, const char *format, ...);
#endif

#endif

