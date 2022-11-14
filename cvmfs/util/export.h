/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_EXPORT_H_
#define CVMFS_UTIL_EXPORT_H_

#ifdef CVMFS_LIBRARY
#define CVMFS_EXPORT __attribute__((visibility("default")))
#else
#define CVMFS_EXPORT
#endif

#endif  // CVMFS_UTIL_EXPORT_H_
