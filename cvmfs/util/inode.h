/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_UTIL_INODE_H_
#define CVMFS_UTIL_INODE_H_
#include "directory_entry.h"  // catalog::DirectoryEntry
#include "duplex_fuse.h"
#include "mountpoint.h"
namespace cvmfs {
bool GetDirentForInode(MountPoint *mountpoint, FileSystem *filesystem,
                       const fuse_ino_t ino, catalog::DirectoryEntry *dirent);
bool GetPathForInode(MountPoint *mountpoint, FileSystem *filesystem,
                     const fuse_ino_t ino, PathString *path);
}  // namespace cvmfs
#endif  // CVMFS_UTIL_INODE_H_

