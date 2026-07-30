/**
 * This file is part of the CernVM File System.
 */

#include "commit_processor.h"

#include <time.h>

#include <cctype>
#include <string>
#include <vector>

#include "catalog_diff_tool.h"
#include "catalog_merge_tool.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "compression/compressor.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "network/sink_path.h"
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

PathString RemoveRepoName(const PathString &lease_path) {
  std::string abs_path = lease_path.ToString();
  const std::string::const_iterator it = std::find(abs_path.begin(),
                                                   abs_path.end(), '/');
  if (it != abs_path.end()) {
    const size_t idx = it - abs_path.begin() + 1;
    return lease_path.Suffix(idx);
  } else {
    return lease_path;
  }
}

bool EditTags(const RepositoryTag &repo_tag, const std::string &repo_name,
              const receiver::Params &params, const std::string &temp_dir,
              const std::string &manifest_path,
              const std::string &public_key_path,
              const std::string &proxy,
              const time_t auto_tag_threshold,
              const bool maintain_undo_tags) {
  swissknife::ArgumentList args;
  args['r'].Reset(new std::string(params.spooler_configuration));
  args['w'].Reset(new std::string(params.stratum0));
  args['t'].Reset(new std::string(temp_dir));
  args['m'].Reset(new std::string(manifest_path));
  args['p'].Reset(new std::string(public_key_path));
  args['f'].Reset(new std::string(repo_name));
  args['e'].Reset(new std::string(params.hash_alg_str));
  args['z'].Reset(new std::string(params.compression_alg_str));
  args['a'].Reset(new std::string(repo_tag.name()));
  args['D'].Reset(new std::string(repo_tag.description()));
  if (maintain_undo_tags) {
    args['x'].Reset(new std::string());
  }
  args['@'].Reset(new std::string(proxy));
  // Remove the tags requested by `cvmfs_server tag -r` in the same history
  // transaction as the (possibly empty) new tag, so a single new history
  // database is published and registered in the reflog for this commit.
  if (!repo_tag.delete_tags().empty()) {
    args['d'].Reset(new std::string(repo_tag.delete_tags()));
  }
  // Remove outdated auto-generated tags in the same history transaction as the
  // tag we are about to add, so that only a single new history database is
  // published (and registered in the reflog) for this commit.
  if (auto_tag_threshold > 0) {
    args['c'].Reset(new std::string(StringifyInt(auto_tag_threshold)));
  }

  const UniquePtr<swissknife::CommandEditTag> edit_cmd(
      new swissknife::CommandEditTag());
  const int ret = edit_cmd->Main(args);

  if (ret) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Error %d editing tags (add: '%s')",
             ret, repo_tag.name().c_str());
    return false;
  }

  return true;
}

}  // namespace

namespace receiver {

// See commit_processor.h for the contract. `now` is injected so the parser is
// deterministic and unit-testable.
time_t ParseRelativeTimespan(const std::string &timespan, time_t now) {
  // Tokenize on whitespace, lower-casing as we go.
  std::vector<std::string> tokens;
  std::string current;
  for (size_t i = 0; i < timespan.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(timespan[i]);
    if (isspace(c)) {
      if (!current.empty()) {
        tokens.push_back(current);
        current.clear();
      }
    } else {
      current += static_cast<char>(tolower(c));
    }
  }
  if (!current.empty()) {
    tokens.push_back(current);
  }

  // Expect exactly "<number> <unit> ago".
  if (tokens.size() != 3 || tokens[2] != "ago") {
    return 0;
  }
  const std::string &number = tokens[0];
  if (number.empty()) {
    return 0;
  }
  for (size_t i = 0; i < number.size(); ++i) {
    if (!isdigit(static_cast<unsigned char>(number[i]))) {
      return 0;
    }
  }
  const int64_t count = String2Int64(number);

  // De-pluralize the unit.
  std::string unit = tokens[1];
  if (!unit.empty() && unit[unit.size() - 1] == 's') {
    unit.resize(unit.size() - 1);
  }

  // Fixed-length units can be subtracted directly.
  int64_t factor = 0;
  if (unit == "sec" || unit == "second") {
    factor = 1;
  } else if (unit == "min" || unit == "minute") {
    factor = 60;
  } else if (unit == "hour") {
    factor = 3600;
  } else if (unit == "day") {
    factor = 86400;
  } else if (unit == "week") {
    factor = 604800;
  }
  if (factor > 0) {
    return now - static_cast<time_t>(count * factor);
  }

  // Calendar units: let mktime() normalize the broken-down time.
  struct tm broken_time;
  localtime_r(&now, &broken_time);
  if (unit == "month") {
    broken_time.tm_mon -= static_cast<int>(count);
    return mktime(&broken_time);
  }
  if (unit == "year") {
    broken_time.tm_year -= static_cast<int>(count);
    return mktime(&broken_time);
  }

  return 0;
}

CommitProcessor::CommitProcessor() : num_errors_(0), statistics_(NULL) { }

CommitProcessor::~CommitProcessor() { }

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
    const std::string &lease_path, const shash::Any &old_root_hash,
    const shash::Any &new_root_hash, const RepositoryTag &tag,
    int64_t lease_expiration, uint64_t *final_revision, bool direct_graft) {
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

  const std::vector<std::string> lease_path_tokens = SplitString(lease_path,
                                                                 '/');

  const std::string repo_name = lease_path_tokens.front();

  Params params;
  if (!GetParamsFromFile(repo_name, &params)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not get configuration parameters.");
    return kError;
  }

  const UniquePtr<ServerTool> server_tool(new ServerTool());

  if (!server_tool->InitDownloadManager(true, params.proxy)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the download manager");
    return kError;
  }

  const std::string public_key = "/etc/cvmfs/keys/" + repo_name + ".pub";
  const std::string certificate = "/etc/cvmfs/keys/" + repo_name + ".crt";
  const std::string private_key = "/etc/cvmfs/keys/" + repo_name + ".key";
  if (!server_tool->InitSignatureManager(public_key, certificate,
                                         private_key)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "CommitProcessor - error: Could not initialize the signature manager");
    return kError;
  }

  const shash::Any manifest_base_hash;
  const UniquePtr<manifest::Manifest> manifest_tgt(
      server_tool->FetchRemoteManifest(params.stratum0, repo_name,
                                       manifest_base_hash));

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


  std::string cache_dir_;
  if (params.use_local_cache) {
    cache_dir_ = "/var/spool/cvmfs/" + repo_name + "/cache.server";
  }

  const std::string spooler_temp_dir = GetSpoolerTempDir(
      params.spooler_configuration);
  assert(!spooler_temp_dir.empty());
  assert(MkdirDeep(spooler_temp_dir + "/receiver", 0755, true));
  const std::string temp_dir_root = spooler_temp_dir
                                    + "/receiver/commit_processor";

  const PathString relative_lease_path = RemoveRepoName(PathString(lease_path));

  std::string new_manifest_path;
  shash::Any new_manifest_hash;

  if (direct_graft) {
    // -- Experimental DirectGraft fast path ----------------------------------
    // Grafts new_root_hash directly into the parent catalog at
    // relative_lease_path via WritableCatalogManager::TryGraftNestedCatalog,
    // bypassing DiffRec entirely.  Only valid when lease_path points to a
    // brand-new directory subtree.  Reached only via the experimental dedicated
    // kCommitGraft reactor request.
    LogCvmfs(kLogReceiver, kLogSyslog,
             "CommitProcessor - lease_path: %s, direct-graft path "
             "(skipping DiffRec)",
             lease_path.c_str());

    const UniquePtr<RaiiTempDir> graft_temp_dir(
        RaiiTempDir::Create(temp_dir_root));
    const std::string graft_temp = graft_temp_dir->dir();

    perf::StatisticsTemplate stats_tmpl("publish", statistics_);
    // Register the FsCounters (n_files_added, n_directories_added, etc.) that
    // StorePublishStatistics expects.  In the DiffRec path these are created by
    // CatalogMergeTool::Run(); DirectGraft bypasses that, so we register them
    // here.  The values stay 0 -- accurate for a graft that adds a whole
    // subtree atomically rather than individual file-level diffs.
    const perf::FsCounters fs_counters(stats_tmpl);
    const upload::SpoolerDefinition definition(
        params.spooler_configuration, params.hash_alg, params.compression_alg,
        params.generate_legacy_bulk_chunks, params.use_file_chunking,
        params.min_chunk_size, params.avg_chunk_size, params.max_chunk_size,
        "dummy_token", "dummy_key");
    const UniquePtr<upload::Spooler> spooler(
        upload::Spooler::Construct(definition, &stats_tmpl));

    const UniquePtr<catalog::WritableCatalogManager> output_mgr(
        new catalog::WritableCatalogManager(
            manifest_tgt->catalog_hash(), params.stratum0, graft_temp,
            spooler.weak_ref(), server_tool->download_manager(),
            params.enforce_limits, params.nested_kcatalog_limit,
            params.root_kcatalog_limit, params.file_mbyte_limit,
            statistics_, params.use_autocatalogs, params.max_weight,
            params.min_weight, cache_dir_));
    if (!output_mgr->Init()) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: Could not initialize catalog manager "
               "for direct-graft");
      return kError;
    }

    if (new_root_hash.IsNull()
        || new_root_hash.suffix != shash::kSuffixCatalog) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: DirectGraft requires a catalog hash");
      return kMergeFailure;
    }

    // Download new_root_hash to a temp file to obtain the size of the catalog
    // database.  TryGraftNestedCatalog downloads the catalog once more
    // internally via LoadFreeCatalog; the probe writes outside the local cache
    // directory, so that second fetch does not hit the cache.
    const std::string catalog_url =
        params.stratum0 + "/data/" + new_root_hash.MakePath();
    const std::string catalog_tmp = graft_temp + "/catalog_size";
    {
      cvmfs::PathSink catalog_sink(catalog_tmp);
      const shash::Any expected = new_root_hash;
      // Decompress while downloading (the content hash is still verified
      // against the compressed stream): nested_catalogs.size holds the size of
      // the catalog database, not of the compressed CAS object.  Compare
      // CommandCheck::FetchCatalog, which validates this column against the
      // size of the decompressed catalog.
      download::JobInfo dl_job(&catalog_url, zip::DecompressionAlg::kGuessDecompression, false, &expected,
                               &catalog_sink);
      const download::Failures dl_ret =
          server_tool->download_manager()->Fetch(&dl_job);
      if (dl_ret != download::kFailOk) {
        LogCvmfs(kLogReceiver, kLogSyslogErr,
                 "CommitProcessor - error: failed to download catalog %s "
                 "for size probe (%d)",
                 catalog_url.c_str(), static_cast<int>(dl_ret));
        unlink(catalog_tmp.c_str());
        return kError;
      }
    }  // PathSink destructor closes the file here
    const int64_t catalog_size = GetFileSize(catalog_tmp);
    unlink(catalog_tmp.c_str());
    // A zero size would be recorded as "unknown" by swissknife check and
    // silently disable its size validation, so reject it here.
    if (catalog_size <= 0) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: empty or unstatable catalog %s",
               catalog_url.c_str());
      return kError;
    }

    // Graft: inserts the nested catalog reference into the parent catalog
    // and propagates the directory entry + counters upward.
    if (!output_mgr->TryGraftNestedCatalog(
            relative_lease_path.ToString(), new_root_hash,
            static_cast<uint64_t>(catalog_size))) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: DirectGraft validation failed for "
               "lease_path: %s",
               lease_path.c_str());
      return kMergeFailure;
    }

    // Commit updates manifest_tgt in-place (new root hash, revision++, etc.)
    if (!output_mgr->Commit(false, 0, manifest_tgt.weak_ref())) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: Could not commit grafted catalog");
      return kMergeFailure;
    }

    // Export the updated manifest to a temp file for CreateNewTag/SigningTool.
    new_manifest_path = CreateTempPath(temp_dir_root, 0600);
    if (!manifest_tgt->Export(new_manifest_path)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: Could not export manifest after graft");
      return kError;
    }
    new_manifest_hash = manifest_tgt->catalog_hash();
    *final_revision = manifest_tgt->revision();

  } else {
    // -- Standard DiffRec path via CatalogMergeTool --------------------------
    LogCvmfs(kLogReceiver, kLogSyslog,
             "CommitProcessor - lease_path: %s, merging catalogs",
             lease_path.c_str());

    CatalogMergeTool<catalog::WritableCatalogManager,
                     catalog::SimpleCatalogManager>
        merge_tool(params.stratum0, old_root_hash, new_root_hash,
                   relative_lease_path, temp_dir_root,
                   server_tool->download_manager(), manifest_tgt.weak_ref(),
                   statistics_, cache_dir_);
    if (!merge_tool.Init()) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "Error: Could not initialize the catalog merge tool");
      return kError;
    }
    if (!merge_tool.Run(params, &new_manifest_path, &new_manifest_hash,
                        final_revision)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - error: Catalog merge failed");
      return kMergeFailure;
    }
  }

  const UniquePtr<RaiiTempDir> raii_temp_dir(
      RaiiTempDir::Create(temp_dir_root));
  const std::string temp_dir = raii_temp_dir->dir();

  // Determine the cutoff below which outdated auto-generated tags are removed.
  // A value sent by the publisher (already an absolute timestamp) takes
  // precedence over the gateway's local CVMFS_AUTO_TAG_TIMESPAN configuration,
  // which is a relative "<N> <unit> ago" timespan resolved here. 0 disables
  // cleanup.
  time_t auto_tag_threshold = final_tag.auto_tag_threshold();
  if (auto_tag_threshold <= 0 && !params.auto_tag_timespan.empty()) {
    auto_tag_threshold = ParseRelativeTimespan(params.auto_tag_timespan,
                                               time(NULL));
    if (auto_tag_threshold <= 0) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "CommitProcessor - warning: could not parse "
               "CVMFS_AUTO_TAG_TIMESPAN '%s' (expected \"<N> <unit> ago\")",
               params.auto_tag_timespan.c_str());
    }
  }
  if (auto_tag_threshold > 0) {
    LogCvmfs(kLogReceiver, kLogSyslog,
             "CommitProcessor - lease_path: %s, cleaning up auto tags "
             "older than %ld",
             lease_path.c_str(), static_cast<long>(auto_tag_threshold));
  }

  // EditTags adds the tag for the new revision, removes any tags requested by
  // `cvmfs_server tag -r`, and, when a cleanup threshold is set, removes the
  // outdated auto tags -- all in the same history transaction. A failure here
  // is fatal: leaving the new revision untagged (or silently keeping stale
  // tags) would be worse than aborting the commit.
  //
  // Only real publish commits should rotate the undo tags (`trunk` and
  // `trunk-previous`). Pure gateway tag edits reuse the current root hash as
  // both old and new hash, so updating undo tags there would incorrectly make
  // `trunk-previous` point at the current HEAD.
  const bool maintain_undo_tags = (old_root_hash != new_root_hash);
  if (!EditTags(final_tag, repo_name, params, temp_dir, new_manifest_path,
                public_key, params.proxy, auto_tag_threshold,
                maintain_undo_tags)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Error editing tags (add: '%s')",
             final_tag.name().c_str());
    return kError;
  }

  // Re-check the lease right before the final, repository-modifying step. The
  // catalog merge and object upload above can be slow, during which the lease
  // may have expired and an overlapping lease may have been granted to another
  // publisher. If the deadline has passed we must not publish: the objects
  // uploaded above stay unreferenced and are reclaimed by garbage collection.
  // lease_expiration already has the gateway's configured safety margin
  // subtracted, so this is a plain comparison against the current time.
  if (static_cast<int64_t>(time(NULL)) >= lease_expiration) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "CommitProcessor - lease_path: %s, lease expired during commit; "
             "skipping publication, uploaded objects will be "
             "garbage-collected",
             lease_path.c_str());
    return kLeaseExpired;
  }

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, signing manifest",
           lease_path.c_str());

  // Add C_N root catalog hash to reflog through SigningTool,
  // so garbage collector can later delete it.
  std::vector<shash::Any> reflog_catalogs;
  reflog_catalogs.push_back(new_root_hash);

  SigningTool signing_tool(server_tool.weak_ref());
  const SigningTool::Result res = signing_tool.Run(
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

  LogCvmfs(kLogReceiver, kLogSyslog,
           "CommitProcessor - lease_path: %s, new root hash: %s",
           lease_path.c_str(), new_manifest_hash.ToString(false).c_str());

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
      upload::SpoolerDefinition sd(params.spooler_configuration, shash::kAny,
                                   params.compression_alg);
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
                                    const std::string &start_time) {
  statistics_ = st;
  statistics_->Register("publish.revision", "");
  start_time_ = start_time;
}

}  // namespace receiver
