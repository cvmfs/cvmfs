/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CVMFS_H_
#define CVMFS_CVMFS_H_

#include <unistd.h>

#include <string>

#include "catalog_mgr.h"
#include "loader.h"

namespace cvmfs {

extern const loader::LoaderExports *loader_exports_;
extern pid_t pid_;

bool Evict(const std::string &path);
bool Pin(const std::string &path);
catalog::LoadError RemountStart();
void GetReloadStatus(bool *drainout_mode, bool *maintenance_mode);
std::string PrintInodeGeneration();
void UnregisterQuotaListener();

}  // namespace cvmfs

#endif  // CVMFS_CVMFS_H_
