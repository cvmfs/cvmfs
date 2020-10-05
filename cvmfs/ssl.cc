/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SSL_H_
#define CVMFS_SSL_H_

#include "ssl.h"

#include <cstdlib>
#include <string>
#include <vector>

#include "duplex_curl.h"
#include "platform.h"
#include "util/posix.h"
#include "util/string.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace {
#endif

bool HasCertificates(std::string directory) {
  if (!DirectoryExists(directory)) return false;

  DIR *dirp = opendir(directory.c_str());
  if (!dirp) return false;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    if ((dirent->d_type == DT_REG) || (dirent->d_type == DT_UNKNOWN)) {
      const std::string filename(dirent->d_name);
      if (HasSuffix(filename, ".pem", /* ignore case = */ true) ||
          HasSuffix(filename, ".crt", /* ignore case = */ true)) {
        closedir(dirp);
        return true;
      }
    }
  }

  closedir(dirp);
  return false;
}

bool Add_CVMFS_SSL_Certificates(CURL *handle) {
  const char *cadir = getenv("X509_CERT_DIR");
  if (!cadir || !*cadir) {
    cadir = "/etc/grid-security/certificates";
  }
  bool certificate_already_added = false;
  if (HasCertificates(cadir)) {
      CURLcode res = curl_easy_setopt(handle, CURLOPT_CAPATH, cadir);
      if (CURLE_OK == res) {
        certificate_already_added = true;
      }
  }
  const char *cabundle = getenv("X509_CERT_BUNDLE");
  if (cabundle && *cabundle) {
    CURLcode res = curl_easy_setopt(handle, CURLOPT_CAINFO, cabundle);
    if (CURLE_OK == res) {
      certificate_already_added = true;
    }
  }

  return certificate_already_added;
}

bool AddSystemSSLCertificates(CURL *handle) {
  std::vector<std::string> cadirs;

  // most systems store the certificates here
  cadirs.push_back("/etc/ssl/certs/");

  cadirs.push_back("/etc/pki/tls/certs/");
  cadirs.push_back("/etc/ssl/");
  cadirs.push_back("/etc/pki/tls/");
  cadirs.push_back("/etc/pki/ca-trust/extracted/pem/");
  cadirs.push_back("/etc/ssl/");

  for (std::vector<std::string>::const_iterator cadir = cadirs.begin();
       cadir != cadirs.end(); ++cadir) {
    if (HasCertificates(*cadir)) {
      CURLcode res = curl_easy_setopt(handle, CURLOPT_CAPATH, (*cadir).c_str());
      if (CURLE_OK == res) {
        return true;
      }
    }
  }

  return false;
}

bool AddSSLCertificates(CURL *handle, bool add_system_certificates) {
  bool already_added = Add_CVMFS_SSL_Certificates(handle);

  if ((already_added == false) && (add_system_certificates == true)) {
    return AddSystemSSLCertificates(handle);
  }
  return already_added;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SSL_H_
