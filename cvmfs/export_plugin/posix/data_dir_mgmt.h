/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_POSIX_DATA_DIR_MGMT_H_
#define CVMFS_EXPORT_PLUGIN_POSIX_DATA_DIR_MGMT_H_

#include <sys/types.h>

#include <string>

void InitializeDataDirectory(struct fs_traversal_context *ctx);

void PosixCheckDirStructure(
  std::string cur_path,
  mode_t mode,
  unsigned int depth = 1);

#endif  // CVMFS_EXPORT_PLUGIN_POSIX_DATA_DIR_MGMT_H_
