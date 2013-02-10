/**
 * This file is part of the CernVM File System.
 */

#include "manifest_fetch.h"

#include <string>
#include <vector>

#include <cassert>

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
  line = GetLineMem(reinterpret_cast<const char *>(whitelist), whitelist_size);
  if (line.length() != 14) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return false;
  }
  payload_bytes += 15;

  // Expiry date
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
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
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if ((expected_repository != "") && ("N" + expected_repository != line)) {
    LogCvmfs(kLogSignature, kLogDebug,
             "repository name on the whitelist does not match "
             "(found %s, expected %s)",
             line.c_str(), expected_repository.c_str());
    return false;
  }
  payload_bytes += line.length() + 1;

  // Search the fingerprint
  bool found = false;
  do {
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
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
               const uint64_t minimum_timestamp, const hash::Any *base_catalog,
               ManifestEnsemble *ensemble)
{
  assert(ensemble);
  const bool probe_hosts = base_url == "";
  Failures result = kFailUnknown;
  int retval;

  const string manifest_url = base_url + string("/.cvmfspublished");
  download::JobInfo download_manifest(&manifest_url, false, probe_hosts, NULL);
  const string whitelist_url = base_url + string("/.cvmfswhitelist");
  download::JobInfo download_whitelist(&whitelist_url, false, probe_hosts, NULL);
  hash::Any certificate_hash;
  string certificate_url = base_url + "/data";  // rest is in manifest
  download::JobInfo download_certificate(&certificate_url, true, probe_hosts,
                                         &certificate_hash);

  retval = download::Fetch(&download_manifest);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "failed to download repository manifest (%d)", retval);
    return kFailLoad;
  }

  // Load Manifest
  ensemble->raw_manifest_buf =
    reinterpret_cast<unsigned char *>(download_manifest.destination_mem.data);
  ensemble->raw_manifest_size = download_manifest.destination_mem.size;
  ensemble->manifest =
    manifest::Manifest::LoadMem(ensemble->raw_manifest_buf,
                                ensemble->raw_manifest_size);
  if (!ensemble->manifest)
    return kFailIncomplete;

  // Basic manifest sanity check
  if (ensemble->manifest->repository_name() != repository_name) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "repository name does not match (found %s, expected %s)",
             ensemble->manifest->repository_name().c_str(),
             repository_name.c_str());
    result = kFailNameMismatch;
    goto cleanup;
  }
  if (ensemble->manifest->root_path() != hash::Md5(hash::AsciiPtr(""))) {
    result = kFailRootMismatch;
    goto cleanup;
  }
  if (ensemble->manifest->publish_timestamp() < minimum_timestamp) {
    result = kFailOutdated;
    goto cleanup;
  }

  // Quick way out: hash matches base catalog
  if (base_catalog && (ensemble->manifest->catalog_hash() == *base_catalog))
    return kFailOk;

  // Load certificate
  certificate_hash = ensemble->manifest->certificate();
  ensemble->FetchCertificate(certificate_hash);
  if (!ensemble->cert_buf) {
    certificate_url += certificate_hash.MakePath(1, 2) + "X";
    retval = download::Fetch(&download_certificate);
    if (retval != download::kFailOk) {
      result = kFailLoad;
        goto cleanup;
    }
    ensemble->cert_buf =
      reinterpret_cast<unsigned char *>(download_certificate.destination_mem.data);
    ensemble->cert_size = download_certificate.destination_mem.size;
  }
  retval = signature::LoadCertificateMem(ensemble->cert_buf,
                                         ensemble->cert_size);
  if (!retval) {
    result = kFailBadCertificate;
    goto cleanup;
  }

  // Verify manifest
  retval = signature::VerifyLetter(ensemble->raw_manifest_buf,
                                   ensemble->raw_manifest_size, false);
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
  ensemble->whitelist_buf =
    reinterpret_cast<unsigned char *>(download_whitelist.destination_mem.data);
  ensemble->whitelist_size = download_whitelist.destination_mem.size;
  retval = signature::VerifyLetter(ensemble->whitelist_buf,
                                   ensemble->whitelist_size, true);
  if (!retval) {
    result = kFailBadWhitelist;
    goto cleanup;
  }
  retval = VerifyWhitelist(ensemble->whitelist_buf, ensemble->whitelist_size,
                           repository_name);
  if (!retval) {
    result = kFailBadWhitelist;
    goto cleanup;
  }

  return kFailOk;

 cleanup:
  delete ensemble->manifest;
  ensemble->manifest = NULL;
  if (ensemble->raw_manifest_buf) free(ensemble->raw_manifest_buf);
  if (ensemble->cert_buf) free(ensemble->cert_buf);
  if (ensemble->whitelist_buf) free(ensemble->whitelist_buf);
  ensemble->raw_manifest_buf = NULL;
  ensemble->cert_buf = NULL;
  ensemble->whitelist_buf = NULL;
  ensemble->raw_manifest_size = 0;
  ensemble->cert_size = 0;
  ensemble->whitelist_size = 0;
  return result;
}

}  // namespace manifest
