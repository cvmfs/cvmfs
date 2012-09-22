/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_FETCH_H_
#define CVMFS_MANIFEST_FETCH_H_

#include <string>

namespace manifest {

class Manifest;

enum Failures {
  kFailOk = 0,
  kFailLoad,
  kFailIncomplete,
  kFailNameMismatch,
  kFailRootMismatch,
  kFailOutdated,
  kFailBadCertificate,
  kFailBadSignature,
  kFailBadWhitelist,
  kFailUnknown,
};


Failures Fetch(const std::string &base_url, const std::string &repository_name,
               const uint64_t minimum_timestamp,
               Manifest **manifest,
               unsigned char **cert_buf, unsigned *cert_size,
               unsigned char **whitelist_buf, unsigned *whitelist_size);

}  // namespace manifest

#endif  // CVMFS_MANIFEST_FETCH_H_
