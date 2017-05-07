/**
 * This file is part of the CernVM File System.
 */

#include "commit_processor.h"

#include <vector>

#include "catalog_merge_tool.h"
#include "compression.h"
#include "logging.h"
#include "manifest.h"
#include "util/algorithm.h"
#include "util/pointer.h"
#include "util/string.h"

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
    const std::string& lease_path, const std::string& old_root_hash_str,
    const std::string& new_root_hash_str) {
  const std::vector<std::string> lease_path_tokens =
      SplitString(lease_path, '/');
  const std::string repo_name = lease_path_tokens.front();

  // Current catalog from the gateway machine
  const std::string manifest_path =
      "/srv/cvmfs/" + repo_name + "/.cvmfspublished";
  UniquePtr<manifest::Manifest> manifest(
      manifest::Manifest::LoadFile(manifest_path));

  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open repository manifest at: %s",
             manifest_path.c_str());
    return kIoError;
  }

  CatalogMergeTool merge_tool(repo_name, old_root_hash_str, new_root_hash_str,
                              manifest->catalog_hash().ToString(true),
                              "/tmp/cvmfs_receiver_merge");

  shash::Any resulting_root_hash;
  if (!merge_tool.Run(&resulting_root_hash)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Catalog merge failed");
    return kMergeError;
  }

  return kSuccess;
}

}  // namespace receiver
