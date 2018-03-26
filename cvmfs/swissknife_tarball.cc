/**
 * This file is part of the CernVM File System
 */

#include "swissknife_tarball.h"

#include "catalog_virtual.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "sync_union_tarball.h"

int swissknife::IngestTarball::Main(const swissknife::ArgumentList &args) {
  SyncParameters params;
  // Initialization
  params.dir_union = MakeCanonicalPath(*args.find('u')->second);
  params.dir_scratch = MakeCanonicalPath(*args.find('s')->second);
  params.dir_rdonly = MakeCanonicalPath(*args.find('c')->second);
  params.dir_temp = MakeCanonicalPath(*args.find('t')->second);
  params.base_hash = shash::MkFromHexPtr(shash::HexPtr(*args.find('b')->second),
                                         shash::kSuffixCatalog);
  params.stratum0 = *args.find('w')->second;
  params.manifest_path = *args.find('o')->second;
  params.spooler_definition = *args.find('r')->second;

  params.public_keys = *args.find('K')->second;
  params.repo_name = *args.find('N')->second;

  params.tar_file = *args.find('T')->second;
  params.base_directory = *args.find('B')->second;
  
  if (args.find('D') != args.end()) {
    params.to_delete = *args.find('D')->second;
  }

  shash::Algorithms hash_algorithm = shash::kSha1;

  params.nested_kcatalog_limit = SyncParameters::kDefaultNestedKcatalogLimit;
  params.root_kcatalog_limit = SyncParameters::kDefaultRootKcatalogLimit;
  params.file_mbyte_limit = SyncParameters::kDefaultFileMbyteLimit;
  
  params.branched_catalog = false; // could be true?
  
  upload::SpoolerDefinition spooler_definition(
      params.spooler_definition, hash_algorithm, params.compression_alg,
      params.generate_legacy_bulk_chunks, params.use_file_chunking,
      params.min_file_chunk_size, params.avg_file_chunk_size,
      params.max_file_chunk_size, params.session_token_file, params.key_file);
  if (params.max_concurrent_write_jobs > 0) {
    spooler_definition.number_of_concurrent_uploads =
        params.max_concurrent_write_jobs;
  }

  upload::SpoolerDefinition spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());

  params.spooler = upload::Spooler::Construct(spooler_definition);
  if (NULL == params.spooler) return 3;
  UniquePtr<upload::Spooler> spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs));
  if (!spooler_catalogs.IsValid()) return 3;
  
  const bool follow_redirects = (args.count('L') > 0);
  if (!InitDownloadManager(follow_redirects)) {
    return 3;
  }

  if (!InitVerifyingSignatureManager(params.public_keys,
                                     params.trusted_certs)) {
    return 3;
  }

  bool with_gateway =
      spooler_definition.driver_type == upload::SpoolerDefinition::Gateway;

  UniquePtr<manifest::Manifest> manifest;
  if (params.branched_catalog) {
    // Throw-away manifest
    manifest = new manifest::Manifest(shash::Any(), 0, "");
  } else if (params.virtual_dir_actions !=
             catalog::VirtualCatalog::kActionNone) {
    manifest = this->OpenLocalManifest(params.manifest_path);
    params.base_hash = manifest->catalog_hash();
  } else {
    if (with_gateway) {
      manifest =
          FetchRemoteManifest(params.stratum0, params.repo_name, shash::Any());
    } else {
      manifest = FetchRemoteManifest(params.stratum0, params.repo_name,
                                     params.base_hash);
    }
  }
  if (!manifest) {
    return 3;
  }

  const std::string old_root_hash = manifest->catalog_hash().ToString(true);

  catalog::WritableCatalogManager catalog_manager(
      params.base_hash, params.stratum0, params.dir_temp, spooler_catalogs,
      download_manager(), params.enforce_limits, params.nested_kcatalog_limit,
      params.root_kcatalog_limit, params.file_mbyte_limit, statistics(),
      params.is_balanced, params.max_weight, params.min_weight);
  catalog_manager.Init();

  publish::SyncMediator mediator(&catalog_manager, &params);

  if (params.virtual_dir_actions == catalog::VirtualCatalog::kActionNone) {
    publish::SyncUnion *sync;

    sync = new publish::SyncUnionTarball(
        &mediator, params.dir_rdonly, params.dir_union, params.dir_scratch,
        params.tar_file, params.base_directory, params.to_delete);
    if (!sync->Initialize()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Initialization of the synchronisation "
               "engine failed");
      return 4;
    }

    sync->Traverse();
  }

  if (!mediator.Commit(manifest.weak_ref())) {
    PrintError("something went wrong during sync");
    return 5;
  }

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

  spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
                                    params.repo_tag);
  delete params.spooler;

  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    return 6;
  }

  return 0;
}
