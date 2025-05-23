/**
 * This file is part of the CernVM File System.
 *
 * Dispatcher for platform specific system/library calls.
 */

#ifndef CVMFS_UTIL_PLATFORM_H_
#define CVMFS_UTIL_PLATFORM_H_

#ifdef __APPLE__
#include "util/platform_osx.h"
#else
#include "util/platform_linux.h"
#endif

#endif  // CVMFS_UTIL_PLATFORM_H_
