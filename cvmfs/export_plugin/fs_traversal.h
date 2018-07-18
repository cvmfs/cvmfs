/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_

#include <string>

#include "fs_traversal.h"
#include "fs_traversal_interface.h"

namespace shrinkwrap {

bool Sync(const char *dir,
          struct fs_traversal *src,
          struct fs_traversal *dest,
          int parallel,
          bool recursive);

int Main(int argc, char **argv);

}  // namespace shrinkwrap

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_H_
