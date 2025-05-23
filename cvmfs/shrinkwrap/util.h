/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_UTIL_H_
#define CVMFS_SHRINKWRAP_UTIL_H_

#include "crypto/hash.h"
#include "libcvmfs.h"

shash::Any HashMeta(const struct cvmfs_attr *stat_info);

void AppendStringToList(char const *str,
                        char ***buf,
                        size_t *listlen,
                        size_t *buflen);

#endif  // CVMFS_SHRINKWRAP_UTIL_H_
