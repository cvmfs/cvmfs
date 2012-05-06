/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_H_
#define CVMFS_MANIFEST_H_

#include <stdint.h>
#include <string>
#include "hash.h"

/**
 * The Manifest is the bootstrap snippet for a repository.  It is stored in
 * .cvmfspublished.
 */
class Manifest {
 public:
  Manifest(const hash::Any &catalog_hash, const std::string &root_path);
  
  bool Export(const std::string &path) const;
  
 private:
  hash::Any catalog_hash_;
  hash::Any micro_catalog_hash_;
  hash::Md5 root_path_;
  uint32_t ttl_;
  uint64_t revision_;
};  // class Manifest

#endif  // CVMFS_MANIFEST_H_
