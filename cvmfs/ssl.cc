/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SSL_H_
#define CVMFS_SSL_H_

#include "ssl.h"

#include <string>
#include <vector>

#include "duplex_curl.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

unsigned int count_ssl_certificates(std::string directory) {
  if (!DirectoryExists(directory)) return 0;

  std::vector<std::string> dotpem = FindFilesBySuffix(directory, ".pem");
  std::vector<std::string> dotcrt = FindFilesBySuffix(directory, ".crt");

  return dotpem.size() + dotcrt.size();
}

bool AddSSLCertificates(CURL *handle) {
  std::vector<std::string> cadirs;
  const char *cadir = getenv("X509_CERT_DIR");
  if (cadir != NULL) {
    cadirs.push_back(std::string(cadir));
  }
  cadirs.push_back("/etc/grid-security/certificates");

  // most systems store the certificates here
  cadirs.push_back("/etc/ssl/certs/");

  cadirs.push_back("/etc/pki/tls/certs/");
  cadirs.push_back("/etc/ssl/");
  cadirs.push_back("/etc/pki/tls/");
  cadirs.push_back("/etc/pki/ca-trust/extracted/pem/");
  cadirs.push_back("/etc/ssl/");

  for (std::vector<std::string>::const_iterator cadir = cadirs.begin();
       cadir != cadirs.end(); ++cadir) {
    if (count_ssl_certificates(*cadir) > 0) {
      CURLcode res = curl_easy_setopt(handle, CURLOPT_CAPATH, (*cadir).c_str());
      if (CURLE_OK == res) {
        return true;
      }
    }
  }

  return false;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SSL_H_
