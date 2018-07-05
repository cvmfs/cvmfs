/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_UTIL_H_
#define CVMFS_EXPORT_PLUGIN_UTIL_H_

#include "hash.h"
#include "libcvmfs.h"

shash::Any HashMeta(struct cvmfs_stat *stat_info);

#endif  // CVMFS_EXPORT_PLUGIN_UTIL_H_
