/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_FETCH_H_
#define CVMFS_MANIFEST_FETCH_H_

#include <string>
#include <cstdlib>

#include "manifest.h"

namespace shash {
struct Any;
}

namespace signature {
class SignatureManager;
}

namespace download {
class DownloadManager;
}

namespace manifest {

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


/**
 * A manifest requires the certificate and the whitelist to be verified.
 * All three (for with the pkcs7 signature of the whitelist) are an ensemble.
 */
struct ManifestEnsemble {
  ManifestEnsemble() {
    manifest = NULL;
    raw_manifest_buf = cert_buf = whitelist_buf = whitelist_pkcs7_buf = NULL;
    raw_manifest_size = cert_size = whitelist_size = whitelist_pkcs7_size = 0;
  }
  virtual ~ManifestEnsemble() {
    delete manifest;
    if (raw_manifest_buf) free(raw_manifest_buf);
    if (cert_buf) free(cert_buf);
    if (whitelist_buf) free(whitelist_buf);
    if (whitelist_pkcs7_buf) free(whitelist_pkcs7_buf);
  }
  // Can be overwritte to fetch certificate from cache
  virtual void FetchCertificate(const shash::Any &hash) { }

  Manifest *manifest;
  unsigned char *raw_manifest_buf;
  unsigned char *cert_buf;
  unsigned char *whitelist_buf;
  unsigned char *whitelist_pkcs7_buf;
  unsigned raw_manifest_size;
  unsigned cert_size;
  unsigned whitelist_size;
  unsigned whitelist_pkcs7_size;
};


Failures Fetch(const std::string &base_url, const std::string &repository_name,
               const uint64_t minimum_timestamp, const shash::Any *base_catalog,
               signature::SignatureManager *signature_manager,
               download::DownloadManager *download_manager,
               ManifestEnsemble *ensemble);

}  // namespace manifest

#endif  // CVMFS_MANIFEST_FETCH_H_
