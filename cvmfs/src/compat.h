#ifndef COMPAT_H
#define COMPAT_H

#ifdef __APPLE__
	#include "compat_macosx.h"
#else
	#include "compat_linux.h"
#endif

#endif
