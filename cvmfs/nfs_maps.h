/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_MAPS_H_
#define CVMFS_NFS_MAPS_H_

#define __STDC_FORMAT_MACROS

#include <inttypes.h>
#include <stdint.h>

#include <string>

#include "shortstring.h"
#include "util/single_copy.h"

class NfsMaps : SingleCopy {
 public:
  virtual ~NfsMaps() { }
  virtual uint64_t GetInode(const PathString &path) = 0;
  virtual bool GetPath(const uint64_t inode, PathString *path) = 0;

  virtual void Spawn() { }
  virtual std::string GetStatistics() { return ""; }
};

#endif  // CVMFS_NFS_MAPS_H_
