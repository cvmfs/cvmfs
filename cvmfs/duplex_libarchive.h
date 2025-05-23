/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_LIBARCHIVE_H_
#define CVMFS_DUPLEX_LIBARCHIVE_H_

#ifdef _BUILT_IN_LIBARCHIVE
#include "archive.h"
#include "archive_entry.h"
#else
#include <archive.h>
#include <archive_entry.h>
#endif

#endif  // CVMFS_DUPLEX_LIBARCHIVE_H_
