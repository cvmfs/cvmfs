/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SERVER_TOOL_H_
#define CVMFS_SERVER_TOOL_H_

#include <string>

#include "crypto/signature.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "reflog.h"
#include "statistics.h"
#include "util/pointer.h"

class ServerTool {
 public:
  ServerTool();
  virtual ~ServerTool();

  bool InitDownloadManager(const bool follow_redirects,
                           const std::string &proxy,
                           const unsigned max_pool_handles = 1);
  bool InitSignatureManager(const std::string &pubkey_path,
                            const std::string &certificate_path = "",
                            const std::string &private_key_path = "");

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

  manifest::Reflog *CreateEmptyReflog(const std::string &temp_directory,
                                      const std::string &repo_name);

  download::DownloadManager *download_manager() const;
  signature::SignatureManager *signature_manager() const;
  perf::Statistics *statistics() { return &statistics_; }
  const perf::Statistics *statistics() const { return &statistics_; }

 protected:
  UniquePtr<download::DownloadManager> download_manager_;
  UniquePtr<signature::SignatureManager> signature_manager_;
  perf::Statistics statistics_;

 private:
  static const unsigned kDownloadTimeout = 60;  // 1 minute
  static const unsigned kDownloadRetries = 3;   // 4 attempts in total
};

#include "server_tool_impl.h"

#endif  // CVMFS_SERVER_TOOL_H_
