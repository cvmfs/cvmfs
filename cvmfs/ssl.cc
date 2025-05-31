/**
 * This file is part of the CernVM File System.
 */

#include "ssl.h"

#include <dirent.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "duplex_curl.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

bool HasCertificates(const std::string &directory) {
  DIR *dirp = opendir(directory.c_str());
  if (!dirp)
    return false;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const std::string filename(directory + "/" + std::string(dirent->d_name));

    platform_stat64 stat;
    if (platform_stat(filename.c_str(), &stat) != 0)
      continue;
    if (!(S_ISREG(stat.st_mode) || S_ISLNK(stat.st_mode)))
      continue;

    if (HasSuffix(filename, ".pem", /* ignore case = */ false)
        || HasSuffix(filename, ".crt", /* ignore case = */ false)) {
      closedir(dirp);
      return true;
    }
  }

  closedir(dirp);
  return false;
}

}  // namespace


SslCertificateStore::SslCertificateStore() {
  const char *ca_path_env = getenv("X509_CERT_DIR");
  if (ca_path_env && *ca_path_env)
    ca_path_ = ca_path_env;
  else
    ca_path_ = "/etc/grid-security/certificates";
  const char *ca_bundle_env = getenv("X509_CERT_BUNDLE");
  if (ca_bundle_env && *ca_bundle_env)
    ca_bundle_ = ca_bundle_env;
}


bool SslCertificateStore::ApplySslCertificatePath(CURL *handle) const {
  const CURLcode res1 =
      curl_easy_setopt(handle, CURLOPT_CAPATH, ca_path_.c_str());
  CURLcode res2 = CURLE_OK;
  if (!ca_bundle_.empty())
    res2 = curl_easy_setopt(handle, CURLOPT_CAINFO, ca_bundle_.c_str());

  return (res1 == CURLE_OK) && (res2 == CURLE_OK);
}


void SslCertificateStore::UseSystemCertificatePath() {
  std::vector<std::string> candidates;

  candidates.push_back("/etc/ssl/certs");
  candidates.push_back("/etc/pki/tls/certs");
  candidates.push_back("/etc/ssl");
  candidates.push_back("/etc/pki/tls");
  candidates.push_back("/etc/pki/ca-trust/extracted/pem");
  candidates.push_back("/etc/ssl");

  for (unsigned i = 0; i < candidates.size(); ++i) {
    if (HasCertificates(candidates[i])) {
      const std::string bundle_candidate = candidates[i] + "/ca-bundle.crt";
      if (ca_bundle_.empty()
          && (FileExists(bundle_candidate)
              || SymlinkExists(bundle_candidate))) {
        ca_bundle_ = bundle_candidate;
      }
      ca_path_ = candidates[i];
      return;
    }
  }

  // fallback
  ca_path_ = candidates[0];
}
