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
#include "util/string.h"

namespace receiver {

CommitProcessor::CommitProcessor(const std::string& temp_dir)
    : temp_dir_(temp_dir), num_errors_(0) {}

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
    const shash::Any& new_root_hash) {
  const std::vector<std::string> lease_path_tokens =
      SplitString(lease_path, '/');

  const std::string repo_name = lease_path_tokens.front();
  const std::string stratum0 = "file:///srv/cvmfs/" + repo_name;

  UniquePtr<ServerTool> server_tool(new ServerTool());

  if (!server_tool->InitDownloadManager(true)) {
    return kIoError;
  }

  const std::string public_key = "/etc/cvmfs/keys/" + repo_name + ".pub";
  const std::string trusted_certs =
      "/etc/cvmfs/repositories.d/" + repo_name + "/trusted_certs";
  if (!server_tool->InitVerifyingSignatureManager(public_key, trusted_certs)) {
    return kIoError;
  }

  shash::Any manifest_base_hash;
  UniquePtr<manifest::Manifest> manifest(server_tool->FetchRemoteManifest(
      stratum0, repo_name, manifest_base_hash));

  // Current catalog from the gateway machine
  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open repository manifest");
    return kIoError;
  }

  CatalogMergeTool<catalog::WritableCatalogManager,
                   catalog::SimpleCatalogManager>
      merge_tool(stratum0, old_root_hash, new_root_hash, temp_dir_,
                 server_tool->download_manager(), manifest.weak_ref());
  if (!merge_tool.Init()) {
    return kIoError;
  }

  Params params;
  if (!GetParamsFromFile(repo_name, &params)) {
    return kIoError;
  }

  std::string new_manifest_path;
  if (!merge_tool.Run(params, &new_manifest_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Catalog merge failed");
    return kMergeError;
  }

  const std::string temp_dir = CreateTempDir(temp_dir_);
  const std::string certificate = "/etc/cvmfs/keys/" + repo_name + ".crt";
  const std::string private_key = "/etc/cvmfs/keys/" + repo_name + ".key";

  // We need to re-initialize the ServerTool component for signing
  server_tool.Destroy();
  server_tool = new ServerTool();

  SigningTool signing_tool(server_tool.weak_ref());
  if (signing_tool.Run(new_manifest_path, stratum0,
                       params.spooler_configuration, temp_dir, certificate,
                       private_key, repo_name, "", "",
                       "/var/spool/cvmfs/" + repo_name + "/reflog.chksum")) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Error signing manifest");
    return kIoError;
  }

  return kSuccess;
}

}  // namespace receiver
