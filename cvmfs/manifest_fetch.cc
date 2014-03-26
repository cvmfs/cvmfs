/**
 * This file is part of the CernVM File System.
 */

#include "manifest_fetch.h"

#include <string>
#include <vector>

#include <cassert>

#include "manifest.h"
#include "hash.h"
#include "download.h"
#include "signature.h"
#include "util.h"

using namespace std;  // NOLINT

namespace manifest {

const int kWlInvalid       = 0x00;
const int kWlVerifyRsa     = 0x01;
const int kWlVerifyPkcs7   = 0x02;
const int kWlVerifyCaChain = 0x04;


/**
 * Checks whether the fingerprint of the loaded PEM certificate is listed on the
 * whitelist stored in a memory chunk.
 */
static int VerifyWhitelist(
  const unsigned char *whitelist,
  const unsigned whitelist_size,
  const string &expected_repository,
  const string &expected_fingerprint,
  signature::SignatureManager *signature_manager)
{
  if (expected_fingerprint == "") {
    LogCvmfs(kLogSignature, kLogDebug, "invalid fingerprint");
    return kWlInvalid;
  }
  LogCvmfs(kLogSignature, kLogDebug,
           "checking certificate with fingerprint %s against whitelist",
           expected_fingerprint.c_str());

  time_t local_timestamp = time(NULL);
  string line;
  unsigned payload_bytes = 0;
  bool verify_pkcs7 = false;
  bool verify_cachain = false;

  // Check timestamp (UTC), ignore issue date (legacy)
  line = GetLineMem(reinterpret_cast<const char *>(whitelist), whitelist_size);
  if (line.length() != 14) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return kWlInvalid;
  }
  payload_bytes += 15;

  // Expiry date
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line.length() != 15) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return kWlInvalid;
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
    return kWlInvalid;
  }
  LogCvmfs(kLogSignature, kLogDebug,  "local time: %s",
           StringifyTime(local_timestamp, true).c_str());
  if (local_timestamp > timestamp) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "whitelist lifetime verification failed, expired");
    return kWlInvalid;
  }
  payload_bytes += 16;

  // Check repository name
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if ((expected_repository != "") && ("N" + expected_repository != line)) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "repository name on the whitelist does not match "
             "(found %s, expected %s)",
             line.c_str(), expected_repository.c_str());
    return kWlInvalid;
  }
  payload_bytes += line.length() + 1;

  // Check for PKCS7
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line == "Vpkcs7") {
    LogCvmfs(kLogSignature, kLogDebug, "whitelist verification: pkcs#7");
    verify_pkcs7 = true;
    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  }

  // Check for CA chain verification
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line == "Wcachain") {
    LogCvmfs(kLogSignature, kLogDebug,
             "whitelist imposes ca chain verification of manifest signature");
    verify_cachain = true;
    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  }

  // Search the fingerprint
  bool found = false;
  do {
    if (line == "--") break;
    if (line.substr(0, expected_fingerprint.length()) == expected_fingerprint)
      found = true;

    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  } while (payload_bytes < whitelist_size);
  payload_bytes += line.length() + 1;

  if (!found) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "the certificate's fingerprint is not on the whitelist");
    return kWlInvalid;
  }

  // Check local blacklist
  vector<string> blacklisted_certificates =
    signature_manager->GetBlacklistedCertificates();
  for (unsigned i = 0; i < blacklisted_certificates.size(); ++i) {
    if (blacklisted_certificates[i].substr(0, 59) == expected_fingerprint) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
               "blacklisted fingerprint (%s)", expected_fingerprint.c_str());
      return kWlInvalid;
    }
  }

  int wl_examination = verify_pkcs7 ? kWlVerifyPkcs7 : kWlVerifyRsa;
  if (verify_cachain)
    wl_examination |= kWlVerifyCaChain;
  return wl_examination;
}


/**
 * Downloads and verifies the manifest, the certificate, and the whitelist.
 * If base_url is empty, uses the probe_hosts feature from download module.
 */
Failures Fetch(const std::string &base_url, const std::string &repository_name,
               const uint64_t minimum_timestamp, const shash::Any *base_catalog,
               signature::SignatureManager *signature_manager,
               download::DownloadManager *download_manager,
               ManifestEnsemble *ensemble)
{
  assert(ensemble);
  const bool probe_hosts = base_url == "";
  Failures result = kFailUnknown;
  int retval;
  download::Failures dl_retval;
  int wl_examination;
  string cert_fingerprint;

  const string manifest_url = base_url + string("/.cvmfspublished");
  download::JobInfo download_manifest(&manifest_url, false, probe_hosts, NULL);
  const string whitelist_url = base_url + string("/.cvmfswhitelist");
  download::JobInfo download_whitelist(&whitelist_url, false, probe_hosts, NULL);
  const string whitelist_pkcs7_url = whitelist_url + string(".pkcs7");
  download::JobInfo download_whitelist_pkcs7(
    &whitelist_pkcs7_url, false, probe_hosts, NULL);
  shash::Any certificate_hash;
  string certificate_url = base_url + "/data";  // rest is in manifest
  download::JobInfo download_certificate(&certificate_url, true, probe_hosts,
                                         &certificate_hash);

  dl_retval = download_manager->Fetch(&download_manifest);
  if (dl_retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "failed to download repository manifest (%d - %s)",
             dl_retval, Code2Ascii(dl_retval));
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
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "repository name does not match (found %s, expected %s)",
             ensemble->manifest->repository_name().c_str(),
             repository_name.c_str());
    result = kFailNameMismatch;
    goto cleanup;
  }
  if (ensemble->manifest->root_path() != shash::Md5(shash::AsciiPtr(""))) {
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
    retval = download_manager->Fetch(&download_certificate);
    if (retval != download::kFailOk) {
      result = kFailLoad;
        goto cleanup;
    }
    ensemble->cert_buf =
      reinterpret_cast<unsigned char *>(download_certificate.destination_mem.data);
    ensemble->cert_size = download_certificate.destination_mem.size;
  }
  retval = signature_manager->LoadCertificateMem(ensemble->cert_buf,
                                                 ensemble->cert_size);
  if (!retval) {
    result = kFailBadCertificate;
    goto cleanup;
  }
  cert_fingerprint =
    signature_manager->FingerprintCertificate(certificate_hash.algorithm);

  // Verify manifest
  retval = signature_manager->VerifyLetter(ensemble->raw_manifest_buf,
                                           ensemble->raw_manifest_size, false);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to verify repository manifest");
    result = kFailBadSignature;
    goto cleanup;
  }

  // Load whitelist and verify
  retval = download_manager->Fetch(&download_whitelist);
  if (retval != download::kFailOk) {
    result = kFailLoad;
    goto cleanup;
  }
  ensemble->whitelist_buf =
    reinterpret_cast<unsigned char *>(download_whitelist.destination_mem.data);
  ensemble->whitelist_size = download_whitelist.destination_mem.size;

  wl_examination =
    VerifyWhitelist(ensemble->whitelist_buf, ensemble->whitelist_size,
                    repository_name, cert_fingerprint, signature_manager);
  if (wl_examination == kWlInvalid) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to verify repository certificate against whitelist");
    result = kFailBadWhitelist;
    goto cleanup;
  }
  if (wl_examination & kWlVerifyRsa) {
    retval = signature_manager->VerifyLetter(ensemble->whitelist_buf,
                                             ensemble->whitelist_size, true);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "failed to verify repository whitelist");
      result = kFailBadWhitelist;
      goto cleanup;
    }
  }
  if (wl_examination & kWlVerifyPkcs7) {
    // Load the separate whitelist pkcs7 structure
    retval = download_manager->Fetch(&download_whitelist_pkcs7);
    if (retval != download::kFailOk) {
      result = kFailLoad;
      goto cleanup;
    }
    ensemble->whitelist_pkcs7_buf =
      reinterpret_cast<unsigned char *>(
        download_whitelist_pkcs7.destination_mem.data);
    ensemble->whitelist_pkcs7_size =
    download_whitelist_pkcs7.destination_mem.size;
    unsigned char *extracted_whitelist;
    unsigned extracted_whitelist_size;
    vector<string> alt_uris;
    retval =
      signature_manager->VerifyPkcs7(ensemble->whitelist_pkcs7_buf,
                                     ensemble->whitelist_pkcs7_size,
                                     &extracted_whitelist,
                                     &extracted_whitelist_size,
                                     &alt_uris);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "failed to verify repository whitelist (pkcs#7): %s",
               signature_manager->GetCryptoError().c_str());
      result = kFailBadWhitelist;
      goto cleanup;
    }

    // Check for subject alternative name matching the repository name
    bool found_uri = false;
    for (unsigned i = 0; i < alt_uris.size(); ++i) {
      LogCvmfs(kLogSignature, kLogDebug, "found pkcs#7 signer uri %s",
               alt_uris[i].c_str());
      if (alt_uris[i] == "cvmfs:" + repository_name) {
        found_uri = true;
        break;
      }
    }
    if (!found_uri) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "failed to find whitelist signer with SAN/URI cvmfs:%s",
               repository_name.c_str());
      result = kFailBadWhitelist;
      goto cleanup;
    }

    // Check once again the extracted whitelist
    LogCvmfs(kLogCvmfs, kLogDebug, "Extracted pkcs#7 whitelist:\n%s",
             string(reinterpret_cast<char *>(extracted_whitelist),
                    extracted_whitelist_size).c_str());
    wl_examination =
      VerifyWhitelist(extracted_whitelist, extracted_whitelist_size,
                      repository_name, cert_fingerprint, signature_manager);
    free(extracted_whitelist);
    if (wl_examination == kWlInvalid) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "failed to verify repository certificate against pkcs#7 "
               "whitelist");
      result = kFailBadWhitelist;
      goto cleanup;
    }
  }
  if (wl_examination & kWlVerifyCaChain) {
    retval = signature_manager->VerifyCaChain();
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "failed to verify manifest signer's certificate");
      result = kFailInvalidCertificate;
      goto cleanup;
    }
  }

  return kFailOk;

 cleanup:
  delete ensemble->manifest;
  ensemble->manifest = NULL;
  if (ensemble->raw_manifest_buf) free(ensemble->raw_manifest_buf);
  if (ensemble->cert_buf) free(ensemble->cert_buf);
  if (ensemble->whitelist_buf) free(ensemble->whitelist_buf);
  if (ensemble->whitelist_pkcs7_buf) free(ensemble->whitelist_pkcs7_buf);
  ensemble->raw_manifest_buf = NULL;
  ensemble->cert_buf = NULL;
  ensemble->whitelist_buf = NULL;
  ensemble->whitelist_pkcs7_buf = NULL;
  ensemble->raw_manifest_size = 0;
  ensemble->cert_size = 0;
  ensemble->whitelist_size = 0;
  ensemble->whitelist_pkcs7_size = 0;
  return result;
}

}  // namespace manifest
