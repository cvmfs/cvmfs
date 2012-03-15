/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CVMFS_H_
#define CVMFS_CVMFS_H_

#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "catalog_mgr.h"

namespace cvmfs {

extern pid_t pid_;
extern std::string *mountpoint_;
extern int max_cache_timeout_;

int ClearFile(const std::string &path);
catalog::LoadError Remount();
unsigned GetMaxTtl();  // in minutes
void SetMaxTtl(const unsigned value);  // in minutes

}  // namespace cvmfs

#endif  // CVMFS_CVMFS_H_
