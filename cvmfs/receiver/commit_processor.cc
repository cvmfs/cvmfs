/**
 * This file is part of the CernVM File System.
 */

#include "commit_processor.h"

#include <time.h>

#include <vector>

#include "catalog_diff_tool.h"
#include "catalog_merge_tool.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "compression.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "params.h"
#include "signing_tool.h"
#include "statistics.h"
#include "statistics_database.h"
#include "swissknife.h"
#include "swissknife_history.h"
#include "util/algorithm.h"
#include "util/logging.h"
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

bool CreateNewTag(const RepositoryTag& repo_tag, const std::string& repo_name,
                  const receiver::Params& params, const std::string& temp_dir,
                  const std::string& manifest_path,
                  const std::string& public_key_path,
                  const std::string& proxy) {
  swissknife::ArgumentList args;
  args['r'].Reset(new std::string(params.spooler_configuration));
  args['w'].Reset(new std::string(params.stratum0));
  args['t'].Reset(new std::string(temp_dir));
  args['m'].Reset(new std::string(manifest_path));
  args['p'].Reset(new std::string(public_key_path));
  args['f'].Reset(new std::string(repo_name));
  args['e'].Reset(new std::string(params.hash_alg_str));
  args['a'].Reset(new std::string(repo_tag.name()));
  args['D'].Reset(new std::string(repo_tag.description()));
  args['x'].Reset(new std::string());
  args['@'].Reset(new std::string(proxy));

  UniquePtr<swissknife::CommandEditTag> edit_cmd(
      new swissknife::CommandEditTag());
  const int ret = edit_cmd->Main(args);

  if (ret) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Error %d creating tag: %s", ret,
             repo_tag.name().c_str());
    return false;
  }

  return true;
}

}  // namespace

namespace receiver {

CommitProcessor::CommitProcessor() : num_errors_(0), statistics_(NULL) {}

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
 * catalog in the repository manifest. The method also signs the updated
 * repository manifest.
 */
CommitProcessor::Result CommitProcessor::Process(
    const std::string& lease_path, const shash::Any& old_root_hash,
    const shash::Any& new_root_hash, const RepositoryTag& tag,
    uint64_t *final_revision) {
  RepositoryTag final_tag = tag;
  // If tag_name is a generic tag, update the time stamp
  if (final_tag.HasGenericName()) {
    final_tag.SetGenericName();
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, old hash: %s, new hash: %s, "
           "tag_name: %s, tag_description: %s",
           lease_path.c_str(), old_root_hash.ToString(true).c_str(),
           new_root_hash.ToString(true).c_str(), final_tag.name().c_str(),
           final_tag.description().c_str());

  const std::vector<std::string> lease_path_tokens =
      SplitString(lease_path, '/');

  const std::string repo_name = lease_path_tokens.front();

  Params params;
  if (!GetParamsFromFile(repo_name, &params)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not get configuration parameters.");
    return kError;
  }

  UniquePtr<ServerTool> server_tool(new ServerTool());

  if (!server_tool->InitDownloadManager(true, params.proxy)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the download manager");
    return kError;
  }

  const std::string public_key = "/etc/cvmfs/keys/" + repo_name + ".pub";
  const std::string certificate = "/etc/cvmfs/keys/" + repo_name + ".crt";
  const std::string private_key = "/etc/cvmfs/keys/" + repo_name + ".key";
  if (!server_tool->InitSignatureManager(public_key, certificate, private_key))
  {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the signature manager");
    return kError;
  }

  shash::Any manifest_base_hash;
  const UniquePtr<manifest::Manifest> manifest_tgt(
    server_tool->FetchRemoteManifest(
      params.stratum0, repo_name, manifest_base_hash));

  // Current catalog from the gateway machine
  if (!manifest_tgt.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - error: Could not open repository manifest");
    return kError;
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, target root hash: %s",
           lease_path.c_str(),
           manifest_tgt->catalog_hash().ToString(false).c_str());

  const std::string spooler_temp_dir =
      GetSpoolerTempDir(params.spooler_configuration);
  assert(!spooler_temp_dir.empty());
  assert(MkdirDeep(spooler_temp_dir + "/receiver", 0666, true));
  const std::string temp_dir_root =
      spooler_temp_dir + "/receiver/commit_processor";

  const PathString relative_lease_path = RemoveRepoName(PathString(lease_path));

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, merging catalogs",
           lease_path.c_str());

  CatalogMergeTool<catalog::WritableCatalogManager,
                   catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, old_root_hash, new_root_hash,
                 relative_lease_path, temp_dir_root,
                 server_tool->download_manager(), manifest_tgt.weak_ref(),
                 statistics_);
  if (!merge_tool.Init()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Error: Could not initialize the catalog merge tool");
    return kError;
  }

  std::string new_manifest_path;
  if (!merge_tool.Run(params, &new_manifest_path, final_revision)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - error: Catalog merge failed");
    return kMergeFailure;
  }

  UniquePtr<RaiiTempDir> raii_temp_dir(RaiiTempDir::Create(temp_dir_root));
  const std::string temp_dir = raii_temp_dir->dir();

  if (!CreateNewTag(final_tag, repo_name, params, temp_dir, new_manifest_path,
                    public_key, params.proxy)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Error creating tag: %s",
             final_tag.name().c_str());
    return kError;
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, signing manifest",
           lease_path.c_str());

  // Add C_N root catalog hash to reflog through SigningTool,
  // so garbage collector can later delete it.
  std::vector<shash::Any> reflog_catalogs;
  reflog_catalogs.push_back(new_root_hash);

  SigningTool signing_tool(server_tool.weak_ref());
  SigningTool::Result res = signing_tool.Run(
      new_manifest_path, params.stratum0, params.spooler_configuration,
      temp_dir, certificate, private_key, repo_name, "", "",
      "/var/spool/cvmfs/" + repo_name + "/reflog.chksum", params.proxy,
      params.garbage_collection, false, false, reflog_catalogs);
  switch (res) {
    case SigningTool::kReflogChecksumMissing:
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: missing reflog.chksum");
      return kMissingReflog;
    case SigningTool::kReflogMissing:
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: missing reflog");
      return kMissingReflog;
    case SigningTool::kError:
    case SigningTool::kInitError:
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: signing manifest");
      return kError;
    case SigningTool::kSuccess:
      LogCvmfs(kLogReceiver, kLogSyslog,
               "CommitProcessor - lease_path: %s, success.",
               lease_path.c_str());
  }

  const UniquePtr<manifest::Manifest> manifest_new(
    manifest::Manifest::LoadFile(new_manifest_path));
  assert(manifest_new.IsValid());
  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, new root hash: %s",
           lease_path.c_str(),
           manifest_new->catalog_hash().ToString(false).c_str());

  // Ensure CVMFS_ROOT_HASH is not set in
  // /var/spool/cvmfs/<REPO_NAME>/client.local
  const std::string fname = "/var/spool/cvmfs/" + repo_name + "/client.local";
  if (truncate(fname.c_str(), 0) < 0) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Could not truncate %s\n",
             fname.c_str());
    return kError;
  }

  StatisticsDatabase *stats_db = StatisticsDatabase::OpenStandardDB(repo_name);
  if (stats_db != NULL) {
    if (!stats_db->StorePublishStatistics(statistics_, start_time_, true)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
        "Could not store publish statistics");
    }
    if (params.upload_stats_db) {
      upload::SpoolerDefinition sd(params.spooler_configuration, shash::kAny);
      upload::Spooler *spooler = upload::Spooler::Construct(sd);
      if (!stats_db->UploadStatistics(spooler)) {
        LogCvmfs(kLogReceiver, kLogSyslogErr,
          "Could not upload statistics DB to upstream storage");
      }
      delete spooler;
    }
    delete stats_db;

  } else {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Could not open statistics DB");
  }

  return kSuccess;
}

void CommitProcessor::SetStatistics(perf::Statistics *st,
                                    const std::string &start_time)
{
  statistics_ = st;
  statistics_->Register("publish.revision", "");
  start_time_ = start_time;
}

}  // namespace receiver
