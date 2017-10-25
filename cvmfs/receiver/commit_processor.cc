/**
 * This file is part of the CernVM File System.
 */

#include "commit_processor.h"

#include <vector>

#include "catalog_diff_tool.h"
#include "catalog_merge_tool.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "compression.h"
#include "download.h"
#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "params.h"
#include "signing_tool.h"
#include "statistics.h"
#include "util/algorithm.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/raii_temp_dir.h"
#include "util/string.h"

namespace {

PathString RemoveRepoName(const PathString& lease_path) {
  std::string abs_path = lease_path.ToString();
  std::string::const_iterator it =
      std::find(abs_path.begin(), abs_path.end(), '/');
  if (it != abs_path.end()) {
    size_t idx = it - abs_path.begin() + 1;
    return lease_path.Suffix(idx);
  } else {
    return lease_path;
  }
}

}  // namespace

namespace receiver {

CommitProcessor::CommitProcessor() : num_errors_(0) {}

CommitProcessor::~CommitProcessor() {}

/**
 * Applies the changes from the new catalog onto the repository.
 *
 * Let:
 *   + C_O = the root catalog of the repository (given by old_root_hash) at
 *           the beginning of the lease, on the release manager machine
 *   + C_N = the root catalog of the repository (given by new_root_hash), on
 *           the release manager machine, with the changes introduced during the
 *           lease
 *   + C_G = the current root catalog of the repository on the gateway machine.
 *
 * This method applies all the changes from C_N, with respect to C_O, onto C_G.
 * The resulting catalog on the gateway machine (C_GN) is then set as root
 * catalog in the repository manifest. The method also signes the updated
 * repository manifest.
 */
CommitProcessor::Result CommitProcessor::Process(
    const std::string& lease_path, const shash::Any& old_root_hash,
    const shash::Any& new_root_hash, const std::string& tag_name) {
  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, old hash: %s, new hash: %s, "
           "tag_name: %s",
           lease_path.c_str(), old_root_hash.ToString(true).c_str(),
           new_root_hash.ToString(true).c_str(), tag_name.c_str());

  const std::vector<std::string> lease_path_tokens =
      SplitString(lease_path, '/');

  const std::string repo_name = lease_path_tokens.front();

  Params params;
  if (!GetParamsFromFile(repo_name, &params)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not get configuration parameters.");
    return kIoError;
  }

  UniquePtr<ServerTool> server_tool(new ServerTool());

  if (!server_tool->InitDownloadManager(true)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the download manager");
    return kIoError;
  }

  const std::string public_key = "/etc/cvmfs/keys/" + repo_name + ".pub";
  const std::string trusted_certs =
      "/etc/cvmfs/repositories.d/" + repo_name + "/trusted_certs";
  if (!server_tool->InitVerifyingSignatureManager(public_key, trusted_certs)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the signature manager");
    return kIoError;
  }

  shash::Any manifest_base_hash;
  UniquePtr<manifest::Manifest> manifest(server_tool->FetchRemoteManifest(
      params.stratum0, repo_name, manifest_base_hash));

  // Current catalog from the gateway machine
  if (!manifest.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - error: Could not open repository manifest");
    return kIoError;
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, target root hash: %s",
           lease_path.c_str(),
           manifest->catalog_hash().ToString(false).c_str());

  const std::string spooler_temp_dir =
      GetSpoolerTempDir(params.spooler_configuration);
  assert(!spooler_temp_dir.empty());
  const std::string temp_dir_root = spooler_temp_dir + "/commit_processor";

  const PathString relative_lease_path = RemoveRepoName(PathString(lease_path));

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, merging catalogs",
           lease_path.c_str());

  CatalogMergeTool<catalog::WritableCatalogManager,
                   catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, old_root_hash, new_root_hash,
                 relative_lease_path, temp_dir_root,
                 server_tool->download_manager(), manifest.weak_ref());
  if (!merge_tool.Init()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Error: Could not initialize the catalog merge tool");
    return kIoError;
  }

  std::string new_manifest_path;
  if (!merge_tool.Run(params, &new_manifest_path)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - error: Catalog merge failed");
    return kMergeError;
  }

  UniquePtr<RaiiTempDir> raii_temp_dir(RaiiTempDir::Create(temp_dir_root));
  const std::string temp_dir = raii_temp_dir->dir();
  const std::string certificate = "/etc/cvmfs/keys/" + repo_name + ".crt";
  const std::string private_key = "/etc/cvmfs/keys/" + repo_name + ".key";

  // We need to re-initialize the ServerTool component for signing
  server_tool.Destroy();
  server_tool = new ServerTool();

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, signing manifest",
           lease_path.c_str());

  SigningTool signing_tool(server_tool.weak_ref());
  if (signing_tool.Run(new_manifest_path, params.stratum0,
                       params.spooler_configuration, temp_dir, certificate,
                       private_key, repo_name, "", "",
                       "/var/spool/cvmfs/" + repo_name + "/reflog.chksum")) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - error: signing manifest");
    return kIoError;
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, success.", lease_path.c_str());

  {
    UniquePtr<ServerTool> server_tool(new ServerTool());

    if (!server_tool->InitDownloadManager(true)) {
      LogCvmfs(
          kLogReceiver, kLogSyslogErr,
          "CommitProcessor - error: Could not initialize the download manager");
      return kIoError;
    }

    const std::string public_key = "/etc/cvmfs/keys/" + repo_name + ".pub";
    const std::string trusted_certs =
        "/etc/cvmfs/repositories.d/" + repo_name + "/trusted_certs";
    if (!server_tool->InitVerifyingSignatureManager(public_key,
                                                    trusted_certs)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: Could not initialize the signature "
               "manager");
      return kIoError;
    }

    shash::Any manifest_base_hash;
    UniquePtr<manifest::Manifest> manifest(server_tool->FetchRemoteManifest(
        params.stratum0, repo_name, manifest_base_hash));

    LogCvmfs(kLogReceiver, kLogSyslog,
             "CommitProcessor - lease_path: %s, new root hash: %s",
             lease_path.c_str(),
             manifest->catalog_hash().ToString(false).c_str());
  }

  return kSuccess;
}

}  // namespace receiver
