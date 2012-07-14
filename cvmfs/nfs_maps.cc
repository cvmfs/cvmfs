/**
 * This file is part of the CernVM File System.
 *
 * NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consequtively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by leveldb.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and consistent during
 * cvmfs restarts.
 *
 * The maps are not accounted for by the cache quota.
 */

#include "nfs_maps.h"
