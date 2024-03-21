/**
 * This file is part of the CernVM File System
 */

#include "swissknife_ingest.h"

#include <fcntl.h>
#include <unistd.h>

#include "catalog_virtual.h"
#include "manifest.h"
#include "statistics.h"
#include "statistics_database.h"
#include "swissknife_capabilities.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "sync_union_tarball.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"

/*
 * Many of the options possible to set in the ArgumentList are not actually used
 * by the ingest command since they are not part of its interface, hence those
 * unused options cannot be set by the shell script. Of course if there is the
 * necessitty those parameters can be added and managed.
 * At the moment this approach worked fine and didn't add much complexity,
 * however if yet another command will need to use a similar approach it would
 * be good to consider creating different options handler for each command.
 */
int swissknife::Ingest::Main(const swissknife::ArgumentList &args) {
  std::string start_time = GetGMTimestamp();

  SyncParameters params;
  params.dir_rdonly = MakeCanonicalPath(*args.find('c')->second);
  params.dir_temp = MakeCanonicalPath(*args.find('t')->second);
  params.base_hash = shash::MkFromHexPtr(shash::HexPtr(*args.find('b')->second),
                                         shash::kSuffixCatalog);
  params.stratum0 = *args.find('w')->second;
  params.manifest_path = *args.find('o')->second;
  params.spooler_definition = *args.find('r')->second;

  params.public_keys = *args.find('K')->second;
  params.repo_name = *args.find('N')->second;

  if (args.find('T') != args.end()) {
    params.tar_file = *args.find('T')->second;
  }
  if (args.find('B') != args.end()) {
    params.base_directory = *args.find('B')->second;
  }
  if (args.find('D') != args.end()) {
    params.to_delete = *args.find('D')->second;
  }

  if (args.find('O') != args.end()) {
    params.generate_legacy_bulk_chunks = true;
  }
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('e') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('e')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }
  if (args.find('Z') != args.end()) {
    params.compression_alg =
        zlib::ParseCompressionAlgorithm(*args.find('Z')->second);
  }
  if (args.find('U') != args.end()) {
    params.uid = static_cast<uid_t>(String2Int64(*args.find('U')->second));
  }
  if (args.find('G') != args.end()) {
    params.gid = static_cast<gid_t>(String2Int64(*args.find('G')->second));
  }

  bool create_catalog = args.find('C') != args.end();

  params.nested_kcatalog_limit = SyncParameters::kDefaultNestedKcatalogLimit;
  params.root_kcatalog_limit = SyncParameters::kDefaultRootKcatalogLimit;
  params.file_mbyte_limit = SyncParameters::kDefaultFileMbyteLimit;

  params.branched_catalog = false;  // could be true?

  if (args.find('P') != args.end()) {
    params.session_token_file = *args.find('P')->second;
  }

  if (args.find('H') != args.end()) {
    params.key_file = *args.find('H')->second;
  }

  const bool upload_statsdb = (args.count('I') > 0);

  perf::StatisticsTemplate publish_statistics("publish", this->statistics());
  StatisticsDatabase *stats_db =
    StatisticsDatabase::OpenStandardDB(params.repo_name);

  upload::SpoolerDefinition spooler_definition(
      params.spooler_definition, hash_algorithm, params.compression_alg,
      params.generate_legacy_bulk_chunks, params.use_file_chunking,
      params.min_file_chunk_size, params.avg_file_chunk_size,
      params.max_file_chunk_size, params.session_token_file, params.key_file);
  if (params.max_concurrent_write_jobs > 0) {
    spooler_definition.number_of_concurrent_uploads =
        params.max_concurrent_write_jobs;
  }

  // Sanitize base_directory, removing any leading or trailing slashes
  // from non-root (!= "/") paths
  params.base_directory = TrimString(params.base_directory, "/", kTrimAll);

  upload::SpoolerDefinition spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());

  params.spooler = upload::Spooler::Construct(spooler_definition,
                                              &publish_statistics);
  if (NULL == params.spooler) return 3;
  UniquePtr<upload::Spooler> spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs,
                                 &publish_statistics));
  if (!spooler_catalogs.IsValid()) return 3;

  const bool follow_redirects = (args.count('L') > 0);
  const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
  if (!InitDownloadManager(follow_redirects, proxy)) {
    return 3;
  }

  if (!InitSignatureManager(params.public_keys)) {
    return 3;
  }

  bool with_gateway =
      spooler_definition.driver_type == upload::SpoolerDefinition::Gateway;

  // This may fail, in which case a warning is printed and the process continues
  ObtainDacReadSearchCapability();

  UniquePtr<manifest::Manifest> manifest;
  if (params.branched_catalog) {
    // Throw-away manifest
    manifest = new manifest::Manifest(shash::Any(), 0, "");
  } else {
    if (with_gateway) {
      manifest =
          FetchRemoteManifest(params.stratum0, params.repo_name, shash::Any());
    } else {
      manifest = FetchRemoteManifest(params.stratum0, params.repo_name,
                                     params.base_hash);
    }
  }
  if (!manifest.IsValid()) {
    return 3;
  }

  const std::string old_root_hash = manifest->catalog_hash().ToString(true);

  catalog::WritableCatalogManager catalog_manager(
      params.base_hash, params.stratum0, params.dir_temp,
      spooler_catalogs.weak_ref(),
      download_manager(), params.enforce_limits, params.nested_kcatalog_limit,
      params.root_kcatalog_limit, params.file_mbyte_limit, statistics(),
      params.is_balanced, params.max_weight, params.min_weight);
  catalog_manager.Init();

  publish::SyncMediator mediator(&catalog_manager, &params, publish_statistics);
  LogCvmfs(kLogPublish, kLogStdout, "Processing changes...");

  publish::SyncUnion *sync = new publish::SyncUnionTarball(
    &mediator, params.dir_rdonly, params.tar_file, params.base_directory,
    params.uid, params.gid, params.to_delete, create_catalog);

  if (!sync->Initialize()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Initialization of the synchronisation "
             "engine failed");
    return 4;
  }

  sync->Traverse();

  if (!params.authz_file.empty()) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "Adding contents of authz file %s to"
             " root catalog.",
             params.authz_file.c_str());
    int fd = open(params.authz_file.c_str(), O_RDONLY);
    if (fd == -1) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Unable to open authz file (%s)"
               "from the publication process: %s",
               params.authz_file.c_str(), strerror(errno));
      return 7;
    }

    std::string new_authz;
    const bool read_successful = SafeReadToString(fd, &new_authz);
    close(fd);

    if (!read_successful) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to read authz file (%s): %s",
               params.authz_file.c_str(), strerror(errno));
      return 8;
    }

    catalog_manager.SetVOMSAuthz(new_authz);
  }

  if (!mediator.Commit(manifest.weak_ref())) {
    PrintError("something went wrong during sync");
    stats_db->StorePublishStatistics(this->statistics(), start_time, false);
    if (upload_statsdb) {
      stats_db->UploadStatistics(params.spooler);
    }
    return 5;
  }

  perf::Counter *revision_counter = statistics()->Register("publish.revision",
                                                  "Published revision number");
  revision_counter->Set(catalog_manager.GetRootCatalog()->revision());

  // finalize the spooler
  LogCvmfs(kLogCvmfs, kLogStdout, "Wait for all uploads to finish");
  params.spooler->WaitForUpload();
  spooler_catalogs->WaitForUpload();
  params.spooler->FinalizeSession(false);

  LogCvmfs(kLogCvmfs, kLogStdout, "Exporting repository manifest");

  // We call FinalizeSession(true) this time, to also trigger the commit
  // operation on the gateway machine (if the upstream is of type "gw").

  // Get the path of the new root catalog
  const std::string new_root_hash = manifest->catalog_hash().ToString(true);

  if (!spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
                                         params.repo_tag)) {
    PrintError("Failed to commit the transaction.");
    stats_db->StorePublishStatistics(this->statistics(), start_time, false);
    if (upload_statsdb) {
      stats_db->UploadStatistics(params.spooler);
    }
    return 9;
  }

  stats_db->StorePublishStatistics(this->statistics(), start_time, true);
  if (upload_statsdb) {
    stats_db->UploadStatistics(params.spooler);
  }

  delete params.spooler;

  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    return 6;
  }

  return 0;
}
