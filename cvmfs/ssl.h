/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SSL_H_
#define CVMFS_SSL_H_

#include <string>

#include "duplex_curl.h"

/**
 * Manages the location of certificates, either system certificates or
 * the ones in $X509_CERT_DIR or /etc/grid-security/certificates
 * On construction, $X509_CERT_DIR has precedence over
 * /etc/grid-security/certificates.
 * $X509_CERT_BUNDLE is checked for a bundle.
 * The path settings can later be overridden by UseSystemCertificatePath();
 * the $X509_CERT_BUNDLE settings are unaffected by UseSystemCertificatePath().
 */
class SslCertificateStore {
 public:
  SslCertificateStore();
  void UseSystemCertificatePath();

  bool ApplySslCertificatePath(CURL *handle) const;
  std::string GetCaPath() const { return ca_path_; }

 private:
  std::string ca_path_;
  std::string ca_bundle_;
};

#endif  // CVMFS_SSL_H_
