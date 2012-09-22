/**
 * This file is part of the CernVM File System.
 */

#include "manifest_fetch.h"

#include <string>
#include <vector>

#include "manifest.h"
#include "download.h"
#include "signature.h"
#include "util.h"

using namespace std;  // NOLINT

namespace manifest {


/**
 * Checks whether the fingerprint of the loaded PEM certificate is listed on the
 * whitelist stored in a memory chunk.
 */
static bool VerifyWhitelist(const unsigned char *whitelist,
                            const unsigned whitelist_size,
                            const string &expected_repository)
{
  const string fingerprint = signature::FingerprintCertificate();
  if (fingerprint == "") {
    LogCvmfs(kLogSignature, kLogDebug, "invalid fingerprint");
    return false;
  }
  LogCvmfs(kLogSignature, kLogDebug,
           "checking certificate with fingerprint %s against whitelist",
           fingerprint.c_str());

  time_t local_timestamp = time(NULL);
  string line;
  unsigned payload_bytes = 0;

  // Check timestamp (UTC), ignore issue date (legacy)
  line = GetLine(reinterpret_cast<const char *>(whitelist), whitelist_size);
  if (line.length() != 14) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return false;
  }
  payload_bytes += 15;

  // Expiry date
  line = GetLine(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                 whitelist_size-payload_bytes);
  if (line.length() != 15) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return false;
  }
  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = String2Int64(line.substr(1, 4))-1900;
  tm_wl.tm_mon = String2Int64(line.substr(5, 2)) - 1;
  tm_wl.tm_mday = String2Int64(line.substr(7, 2));
  tm_wl.tm_hour = String2Int64(line.substr(9, 2));
  tm_wl.tm_min = tm_wl.tm_sec = 0;  // exact on hours level
  time_t timestamp = timegm(&tm_wl);
  LogCvmfs(kLogSignature, kLogDebug,
           "whitelist UTC expiry timestamp in localtime: %s",
           StringifyTime(timestamp, false).c_str());
  if (timestamp < 0) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp");
    return false;
  }
  LogCvmfs(kLogSignature, kLogDebug,  "local time: %s",
           StringifyTime(local_timestamp, true).c_str());
  if (local_timestamp > timestamp) {
    LogCvmfs(kLogSignature, kLogDebug,
             "whitelist lifetime verification failed, expired");
    return false;
  }
  payload_bytes += 16;

  // Check repository name
  line = GetLine(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                 whitelist_size-payload_bytes);
  if ((expected_repository != "") && ("N" + expected_repository != line)) {
    LogCvmfs(kLogSignature, kLogDebug,
             "repository name does not match (found %s, expected %s)",
             line.c_str(), expected_repository.c_str());
    return false;
  }
  payload_bytes += line.length() + 1;

  // Search the fingerprint
  bool found = false;
  do {
    line = GetLine(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                   whitelist_size-payload_bytes);
    if (line == "--") break;
    if (line.substr(0, 59) == fingerprint)
      found = true;
    payload_bytes += line.length() + 1;
  } while (payload_bytes < whitelist_size);
  payload_bytes += line.length() + 1;

  if (!found) {
    LogCvmfs(kLogSignature, kLogDebug,
             "the certificate's fingerprint is not on the whitelist");
    return false;
  }

  // Check local blacklist
  vector<string> blacklisted_certificates =
    signature::GetBlacklistedCertificates();
  for (unsigned i = 0; i < blacklisted_certificates.size(); ++i) {
    if (blacklisted_certificates[i].substr(0, 59) == fingerprint) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslog,
               "blacklisted fingerprint (%s)", fingerprint.c_str());
      return false;
    }
  }

  return true;
}


/**
 * Downloads and verifies the manifest, the certificate, and the whitelist.
 * If base_url is empty, uses the probe_hosts feature from download module.
 */
Failures Fetch(const std::string &base_url, const std::string &repository_name,
               const uint64_t minimum_timestamp,
               Manifest **manifest,
               unsigned char **cert_buf, unsigned *cert_size,
               unsigned char **whitelist_buf, unsigned *whitelist_size)
{
  const bool probe_hosts = base_url == "";
  Failures result = kFailUnknown;
  *cert_buf = NULL;
  *cert_size = 0;
  *whitelist_buf = NULL;
  *whitelist_size = 0;
  int retval;

  const string manifest_url = base_url + string("/.cvmfspublished");
  download::JobInfo download_manifest(&manifest_url, false, probe_hosts, NULL);
  const string whitelist_url = base_url + string("/.cvmfswhitelist");
  download::JobInfo download_whitelist(&whitelist_url, false, probe_hosts, NULL);

  retval = download::Fetch(&download_manifest);
  if (retval != download::kFailOk)
    return kFailLoad;

  // Load Manifest
  const char *manifest_buf = download_manifest.destination_mem.data;
  *manifest = manifest::Manifest::LoadMem(
    reinterpret_cast<const unsigned char *>(manifest_buf),
    download_manifest.destination_mem.size);
  if (!manifest)
    return kFailIncomplete;

  // Prepare certificate parameters
  hash::Any certificate_hash = (*manifest)->certificate();
  string certificate_url = base_url + "/data" +
  certificate_hash.MakePath(1, 2) + "X";
  download::JobInfo download_certificate(&certificate_url, true, probe_hosts,
                                         &certificate_hash);

  // Basic manifest sanity check
  if ((*manifest)->repository_name() != repository_name) {
    result = kFailNameMismatch;
    goto cleanup;
  }
  if ((*manifest)->root_path() != hash::Md5(hash::AsciiPtr(""))) {
    result = kFailRootMismatch;
    goto cleanup;
  }
  if ((*manifest)->publish_timestamp() < minimum_timestamp) {
    result = kFailOutdated;
    goto cleanup;
  }

  // Load certificate
  retval = download::Fetch(&download_certificate);
  if (retval != download::kFailOk) {
    result = kFailLoad;
    goto cleanup;
  }
  *cert_buf =
    reinterpret_cast<unsigned char *>(download_certificate.destination_mem.data);
  *cert_size = download_certificate.destination_mem.size;
  retval = signature::LoadCertificateMem(*cert_buf, *cert_size);
  if (!retval) {
    result = kFailBadCertificate;
    goto cleanup;
  }

  // Verify manifest
  retval = signature::VerifyLetter(
    reinterpret_cast<unsigned char *>(download_manifest.destination_mem.data),
    download_manifest.destination_mem.size, false);
  if (!retval) {
    result = kFailBadSignature;
    goto cleanup;
  }

  // Load whitelist and verify
  retval = download::Fetch(&download_whitelist);
  if (retval != download::kFailOk) {
    result = kFailLoad;
    goto cleanup;
  }
  *whitelist_buf =
    reinterpret_cast<unsigned char *>(download_whitelist.destination_mem.data);
  *whitelist_size = download_whitelist.destination_mem.size;
  retval = signature::VerifyLetter(*whitelist_buf, *whitelist_size, true);
  if (!retval) {
    result = kFailBadWhitelist;
    goto cleanup;
  }
  retval = VerifyWhitelist(*whitelist_buf, *whitelist_size, repository_name);
  if (!retval) {
    result = kFailBadWhitelist;
    goto cleanup;
  }

  return kFailOk;

 cleanup:
  delete *manifest;
  *manifest = NULL;
  if (*cert_buf)
    free(*cert_buf);
  if (*whitelist_buf)
    free(*whitelist_buf);
  *cert_buf = NULL;
  *cert_size = 0;
  *whitelist_buf = NULL;
  *whitelist_size = 0;
  return result;
}

}  // namespace manifest
