/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BRIDGE_MIGRATE_H_
#define CVMFS_BRIDGE_MIGRATE_H_

#include "util/export.h"

extern "C" {

CVMFS_EXPORT void *cvm_bridge_migrate_nfiles_ctr_v1v2s(void *v1);
CVMFS_EXPORT void cvm_bridge_free_nfiles_ctr_v1(void *v1);

CVMFS_EXPORT void *cvm_bridge_migrate_inode_generation_v1v2s(void *v1);
CVMFS_EXPORT void cvm_bridge_free_inode_generation_v1(void *v1);

}  // extern "C"

#endif  // CVMFS_BRIDGE_MIGRATE_H_
