/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_SCAN_INTERFACE_H_
#define CVMFS_SHRINKWRAP_SCAN_INTERFACE_H_

#include <set>
#include <string>

struct fs_traversal_scan_context {
  std::set<std::string> data_dir;
};



struct fs_traversal *scan_get_interface();

#endif  // CVMFS_SHRINKWRAP_SCAN_INTERFACE_H_
