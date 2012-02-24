/**
 * This file is part of the CernVM File System.
 *
 * Dispatcher for platform specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_H_
#define CVMFS_PLATFORM_H_

#ifdef __APPLE__
#include "compat_macosx.h"
#else
#include "compat_linux.h"
#endif

#endif  // CVMFS_PLATFORM_H_
