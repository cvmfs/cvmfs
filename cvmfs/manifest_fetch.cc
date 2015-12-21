/**
 * This file is part of the CernVM File System.
 */

#include "manifest_fetch.h"

#include <string>
#include <vector>

#include <cassert>

#include "download.h"
#include "hash.h"
#include "manifest.h"
#include "signature.h"
#include "util.h"
#include "whitelist.h"

using namespace std;  // NOLINT

namespace manifest {

const int kWlInvalid       = 0x00;
const int kWlVerifyRsa     = 0x01;
const int kWlVerifyPkcs7   = 0x02;
const int kWlVerifyCaChain = 0x04;


/**
 * Downloads and verifies the manifest, the certificate, and the whitelist.
 * If base_url is empty, uses the probe_hosts feature from download manager.
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
  bool retval_b;
  download::Failures retval_dl;
  whitelist::Failures retval_wl;
  whitelist::Whitelist whitelist(repository_name,
                                 download_manager,
                                 signature_manager);

  const string manifest_url = base_url + string("/.cvmfspublished");
  download::JobInfo download_manifest(&manifest_url, false, probe_hosts, NULL);
  shash::Any certificate_hash;
  string certificate_url = base_url + "/";  // rest is in manifest
  download::JobInfo download_certificate(&certificate_url, true, probe_hosts,
                                         &certificate_hash);

  retval_dl = download_manager->Fetch(&download_manifest);
  if (retval_dl != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "failed to download repository manifest (%d - %s)",
             retval_dl, download::Code2Ascii(retval_dl));
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
    certificate_url += ensemble->manifest->MakeCertificatePath();
    retval_dl = download_manager->Fetch(&download_certificate);
    if (retval_dl != download::kFailOk) {
      result = kFailLoad;
        goto cleanup;
    }
    ensemble->cert_buf = reinterpret_cast<unsigned char *>(
      download_certificate.destination_mem.data);
    ensemble->cert_size = download_certificate.destination_mem.size;
  }
  retval_b = signature_manager->LoadCertificateMem(ensemble->cert_buf,
                                                   ensemble->cert_size);
  if (!retval_b) {
    result = kFailBadCertificate;
    goto cleanup;
  }

  // Verify manifest
  retval_b = signature_manager->VerifyLetter(ensemble->raw_manifest_buf,
                                             ensemble->raw_manifest_size,
                                             false);
  if (!retval_b) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to verify repository manifest");
    result = kFailBadSignature;
    goto cleanup;
  }

  // Load whitelist and verify
  retval_wl = whitelist.Load(base_url);
  if (retval_wl != whitelist::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "whitelist verification failed (%d): %s",
             retval_wl, whitelist::Code2Ascii(retval_wl));
    result = kFailBadWhitelist;
    goto cleanup;
  }

  retval_wl = whitelist.VerifyLoadedCertificate();
  if (retval_wl != whitelist::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to verify repository signature against whitelist (%d): %s",
             retval_wl, whitelist::Code2Ascii(retval_wl));
    result = kFailInvalidCertificate;
    goto cleanup;
  }

  whitelist.CopyBuffers(&ensemble->whitelist_size,
                        &ensemble->whitelist_buf,
                        &ensemble->whitelist_pkcs7_size,
                        &ensemble->whitelist_pkcs7_buf);

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
