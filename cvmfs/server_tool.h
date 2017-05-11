/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SERVER_TOOL_H_
#define CVMFS_SERVER_TOOL_H_

#include "download.h"
#include "manifest_fetch.h"
#include "reflog.h"
#include "signature.h"
#include "statistics.h"
#include "util/pointer.h"

class ServerTool {
 public:
  ServerTool();
  virtual ~ServerTool();

  bool InitDownloadManager(const bool follow_redirects,
                           const unsigned max_pool_handles = 1,
                           const bool use_system_proxy = true);
  bool InitVerifyingSignatureManager(const std::string &pubkey_path,
                                     const std::string &trusted_certs = "");
  bool InitSigningSignatureManager(const std::string &certificate_path,
                                   const std::string &private_key_path,
                                   const std::string &private_key_password);

  manifest::Manifest *OpenLocalManifest(const std::string path) const;
  manifest::Failures FetchRemoteManifestEnsemble(
      const std::string &repository_url, const std::string &repository_name,
      manifest::ManifestEnsemble *ensemble) const;
  manifest::Manifest *FetchRemoteManifest(
      const std::string &repository_url, const std::string &repository_name,
      const shash::Any &base_hash = shash::Any()) const;

  template <class ObjectFetcherT>
  manifest::Reflog *FetchReflog(ObjectFetcherT *object_fetcher,
                                const std::string &repo_name,
                                const shash::Any &reflog_hash);

  download::DownloadManager *download_manager() const;
  signature::SignatureManager *signature_manager() const;
  perf::Statistics *statistics() { return &statistics_; }

 private:
  static const unsigned kDownloadTimeout = 20;  // 20 seconds
  static const unsigned kDownloadRetries = 1;   // 2 attempts in total
  UniquePtr<download::DownloadManager> download_manager_;
  UniquePtr<signature::SignatureManager> signature_manager_;
  perf::Statistics statistics_;
};

#include "server_tool_impl.h"

#endif  // CVMFS_SERVER_TOOL_H_
