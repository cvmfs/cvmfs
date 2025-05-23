/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_POSIX_DATA_DIR_MGMT_H_
#define CVMFS_SHRINKWRAP_POSIX_DATA_DIR_MGMT_H_

#include <sys/types.h>

#include <string>

/**
 * The functions in this file handle the management of the .data directory
 * structure in the POSIX File System Traversal interface.
 *
 * The .data directory structure contains (meta)content-adressable links to all
 * inodes of the exported file system. This is used for deduplication.
 */

/**
 * Method which initializes the .data directory with all subdirectories.
 * Depending on kDirLevels and kDigitsPerDirLevel (defined in helpers.h)
 * this process might be very slow.
 */
void InitializeDataDirectory(struct fs_traversal_context *ctx);

#endif  // CVMFS_SHRINKWRAP_POSIX_DATA_DIR_MGMT_H_
