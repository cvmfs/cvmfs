/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_MAPS_H_
#define CVMFS_NFS_MAPS_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#include <string>

#include "shortstring.h"
#include "util/logging.h"
#include "util/single_copy.h"

class NfsMaps : SingleCopy {
 public:
  virtual ~NfsMaps() { }
  virtual uint64_t GetInode(const PathString &path) = 0;
  virtual bool GetPath(const uint64_t inode, PathString *path) = 0;

  /**
   * Ensures that NFS maps inodes have the form an+b so that we can have
   * non-overlapping inodes for independent repositories.
   */
  virtual void SetInodeResidue(unsigned residue_class, unsigned remainder) {
    LogCvmfs(kLogNfsMaps, kLogSyslogWarn,
             "Warning: interleaved inode mode unsupported");
  }

  virtual void Spawn() { }
  virtual std::string GetStatistics() { return ""; }
};

#endif  // CVMFS_NFS_MAPS_H_
