/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_SHARED_MAPS_H_
#define CVMFS_NFS_SHARED_MAPS_H_

#include <string>

#include "hash.h"
#include "nfs_maps.h"
#include "shortstring.h"

namespace nfs_shared_maps {

bool Init(const std::string &db_dir, const uint64_t root_inode,
          const bool rebuild);
void Fini();
void Spawn();

uint64_t GetInode(const PathString &path);
bool GetPath(const uint64_t inode, PathString *path);

std::string GetStatistics();

}  // namespace nfs_shared_maps

#endif  // CVMFS_NFS_SHARED_MAPS_H_
