/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_MAPS_H_
#define CVMFS_NFS_MAPS_H_

#include <string>

namespace nfs_maps {

bool Init(const std::string &leveldb_dir);
void Fini();

}  // namespace nfs_maps

#endif  // CVMFS_NFS_MAPS_H_
