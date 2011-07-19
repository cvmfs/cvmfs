#ifndef COMPAT_H
#define COMPAT_H

/* -------------------------------------------- 
 *
 *  dummy functions... TODO!!
 *
 * -------------------------------------------- */

#ifdef __APPLE__
	#include "compat_macosx.h"
#else
	#include "compat_linux.h"
#endif

#endif