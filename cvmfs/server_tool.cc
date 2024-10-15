/**
 * This file is part of the CernVM File System.
 */

#include "server_tool.h"

#include "util/posix.h"

ServerTool::ServerTool() {}

ServerTool::~ServerTool() {
  if (signature_manager_.IsValid()) {
    signature_manager_->Fini();
  }
}

bool ServerTool::InitDownloadManager(const bool follow_redirects,
                                     const std::string &proxy,
                                     const unsigned max_pool_handles) {
  if (download_manager_.IsValid()) {
    return true;
  }

  download_manager_ = new download::DownloadManager(max_pool_handles,
                            perf::StatisticsTemplate("download", statistics()));
  assert(download_manager_.IsValid());

  download_manager_->SetTimeout(kDownloadTimeout, kDownloadTimeout);
  download_manager_->SetRetryParameters(kDownloadRetries, 2000, 5000);
  download_manager_->UseSystemCertificatePath();

  if (proxy != "") {
    download_manager_->SetProxyChain(proxy, "",
                                     download::DownloadManager::kSetProxyBoth);
  }

  if (follow_redirects) {
    download_manager_->EnableRedirects();
  }

  return true;
}

bool ServerTool::InitSignatureManager(
    const std::string &pubkey_path,
    const std::string &certificate_path,
    const std::string &private_key_path)
{
  if (signature_manager_.IsValid()) {
    return true;
  }

  signature_manager_ = new signature::SignatureManager();
  assert(signature_manager_.IsValid());
  signature_manager_->Init();

  // We may not have a public key. In this case, the signature manager
  // can only be used for signing, not for verification.
  if (!pubkey_path.empty()) {
    if (!signature_manager_->LoadPublicRsaKeys(pubkey_path)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repo key '%s'",
               pubkey_path.c_str());
      return false;
    }
  }

  // We may not have a certificate and key. In this case, the signature manager
  // can only be used for verification, not for signing.
  if (certificate_path.empty())
    return true;

  if (!signature_manager_->LoadCertificatePath(certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate '%s'",
             certificate_path.c_str());
    return false;
  }

  // Load private key
  if (!signature_manager_->LoadPrivateKeyPath(private_key_path, "")) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load private key '%s' (%s)",
             private_key_path.c_str(),
             signature_manager_->GetCryptoError().c_str());
    return false;
  }

  if (!signature_manager_->KeysMatch()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "the private key '%s' doesn't seem to match certificate '%s' (%s)",
             private_key_path.c_str(), certificate_path.c_str(),
             signature_manager_->GetCryptoError().c_str());
    signature_manager_->UnloadPrivateKey();
    return false;
  }

  return true;
}

download::DownloadManager *ServerTool::download_manager() const {
  assert(download_manager_.IsValid());
  return download_manager_.weak_ref();
}

signature::SignatureManager *ServerTool::signature_manager() const {
  assert(signature_manager_.IsValid());
  return signature_manager_.weak_ref();
}

manifest::Manifest *ServerTool::OpenLocalManifest(
    const std::string path) const {
  return manifest::Manifest::LoadFile(path);
}

manifest::Failures ServerTool::FetchRemoteManifestEnsemble(
    const std::string &repository_url, const std::string &repository_name,
    manifest::ManifestEnsemble *ensemble) const {
  const uint64_t minimum_timestamp = 0;
  const shash::Any *base_catalog = NULL;
  return manifest::Fetch(repository_url, repository_name, minimum_timestamp,
                         base_catalog, signature_manager(), download_manager(),
                         ensemble);
}

manifest::Manifest *ServerTool::FetchRemoteManifest(
    const std::string &repository_url, const std::string &repository_name,
    const shash::Any &base_hash) const {
  manifest::ManifestEnsemble manifest_ensemble;
  UniquePtr<manifest::Manifest> manifest;

  // fetch (and verify) the manifest
  const manifest::Failures retval = FetchRemoteManifestEnsemble(
      repository_url, repository_name, &manifest_ensemble);

  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to fetch repository manifest "
             "(%d - %s)",
             retval, manifest::Code2Ascii(retval));
    return NULL;
  } else {
    // copy-construct a fresh manifest object because ManifestEnsemble will
    // free manifest_ensemble.manifest when it goes out of scope
    manifest = new manifest::Manifest(*manifest_ensemble.manifest);
  }

  // check if manifest fetching was successful
  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return NULL;
  }

  // check the provided base hash of the repository if provided
  if (!base_hash.IsNull() && manifest->catalog_hash() != base_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "base hash does not match manifest "
             "(found: %s expected: %s)",
             manifest->catalog_hash().ToString().c_str(),
             base_hash.ToString().c_str());
    return NULL;
  }

  // return the fetched manifest (releasing pointer ownership)
  return manifest.Release();
}

manifest::Reflog *ServerTool::CreateEmptyReflog(
    const std::string &temp_directory, const std::string &repo_name) {
  // create a new Reflog if there was none found yet
  const std::string tmp_path_prefix = temp_directory + "/new_reflog";
  const std::string tmp_path = CreateTempPath(tmp_path_prefix, 0600);

  LogCvmfs(kLogCvmfs, kLogDebug, "creating new reflog '%s' for %s",
           tmp_path.c_str(), repo_name.c_str());
  return manifest::Reflog::Create(tmp_path, repo_name);
}
