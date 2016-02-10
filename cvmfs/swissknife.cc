/**
 * This file is part of the CernVM File System
 *
 * This tool acts as an entry point for all the server-related
 * cvmfs tasks, such as uploading files and checking the sanity of
 * a repository.
 */

#include "cvmfs_config.h"
#include "swissknife.h"

#include <unistd.h>

#include <cassert>
#include <vector>

#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"

using namespace std;  // NOLINT

namespace swissknife {

Command::~Command() {
  if (download_manager_.IsValid()) {
    download_manager_->Fini();
  }

  if (signature_manager_.IsValid()) {
    signature_manager_->Fini();
  }
}


bool Command::InitDownloadManager(const bool     follow_redirects,
                                  const unsigned max_pool_handles,
                                  const bool     use_system_proxy) {
  if (download_manager_.IsValid()) {
    return true;
  }

  download_manager_ = new download::DownloadManager();
  assert(download_manager_);
  download_manager_->Init(max_pool_handles, use_system_proxy, statistics());

  if (follow_redirects) {
    download_manager_->EnableRedirects();
  }

  return true;
}

bool Command::InitVerifyingSignatureManager(const std::string &pubkey_path,
                                            const std::string &trusted_certs) {
  if (signature_manager_.IsValid()) {
    return true;
  }

  signature_manager_ = new signature::SignatureManager();
  assert(signature_manager_);
  signature_manager_->Init();

  if (!signature_manager_->LoadPublicRsaKeys(pubkey_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repo key '%s'",
             pubkey_path.c_str());
    return false;
  }

  if (!trusted_certs.empty() &&
      !signature_manager_->LoadTrustedCaCrl(trusted_certs)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load trusted certificates");
    return false;
  }

  return true;
}


bool Command::InitSigningSignatureManager(
                                      const std::string &certificate_path,
                                      const std::string &private_key_path,
                                      const std::string &private_key_password) {
  if (signature_manager_.IsValid()) {
    return true;
  }

  signature_manager_ = new signature::SignatureManager();
  assert(signature_manager_);
  signature_manager_->Init();

  // Load certificate
  if (!signature_manager_->LoadCertificatePath(certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate '%s'",
                                    certificate_path.c_str());
    return false;
  }

  // Load private key
  if (!signature_manager_->LoadPrivateKeyPath(private_key_path,
                                              private_key_password)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load private key '%s' (%s)",
             private_key_path.c_str(),
             signature_manager_->GetCryptoError().c_str());
    return false;
  }

  if (!signature_manager_->KeysMatch()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "the private key '%s' doesn't seem to match certificate '%s' (%s)",
             private_key_path.c_str(),
             certificate_path.c_str(),
             signature_manager_->GetCryptoError().c_str());
    signature_manager_->UnloadPrivateKey();
    return false;
  }

  return true;
}


download::DownloadManager* Command::download_manager()  const {
  assert(download_manager_.IsValid());
  return download_manager_.weak_ref();
}

signature::SignatureManager* Command::signature_manager() const {
  assert(signature_manager_.IsValid());
  return signature_manager_.weak_ref();
}


manifest::Manifest* Command::OpenLocalManifest(const std::string path) const {
  return manifest::Manifest::LoadFile(path);
}


manifest::Failures Command::FetchRemoteManifestEnsemble(
                             const std::string &repository_url,
                             const std::string &repository_name,
                                   manifest::ManifestEnsemble *ensemble) const {
  const uint64_t    minimum_timestamp = 0;
  const shash::Any *base_catalog      = NULL;
  return manifest::Fetch(repository_url,
                         repository_name,
                         minimum_timestamp,
                         base_catalog,
                         signature_manager(),
                         download_manager(),
                         ensemble);
}


manifest::Manifest* Command::FetchRemoteManifest(
                                          const std::string &repository_url,
                                          const std::string &repository_name,
                                          const shash::Any  &base_hash) const {
  manifest::ManifestEnsemble manifest_ensemble;
  UniquePtr<manifest::Manifest> manifest;

  // fetch (and verify) the manifest
  const manifest::Failures retval =
                              FetchRemoteManifestEnsemble(repository_url,
                                                          repository_name,
                                                          &manifest_ensemble);

  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch repository manifest "
                                    "(%d - %s)",
             retval, manifest::Code2Ascii(retval));
    return NULL;
  } else {
    // copy-construct a fresh manifest object because ManifestEnsemble will
    // free manifest_ensemble.manifest when it goes out of scope
    manifest = new manifest::Manifest(*manifest_ensemble.manifest);
  }

  // check if manifest fetching was successful
  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return NULL;
  }

  // check the provided base hash of the repository if provided
  if (!base_hash.IsNull() && manifest->catalog_hash() != base_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr, "base hash does not match manifest "
                                    "(found: %s expected: %s)",
             manifest->catalog_hash().ToString().c_str(),
             base_hash.ToString().c_str());
    return NULL;
  }

  // return the fetched manifest (releasing pointer ownership)
  return manifest.Release();
}

}  // namespace swissknife
