/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_

#include <string>

#include "fs_traversal.h"
#include "fs_traversal_interface.h"

namespace shrinkwrap {

class CommandExport {
 public:
  ~CommandExport() { }
  int Main();
  bool Traverse(const char *dir,
                struct fs_traversal *src,
                struct fs_traversal *dest,
                int parallel,
                bool recursive);
};

}  // namespace shrinkwrap

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
