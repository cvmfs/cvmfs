/**
 * This file is part of the CernVM File System
 *
 * This tool figures out the changes made to a cvmfs repository by means
 * of a union file system mounted on top of a cvmfs volume.
 * We take all three volumes (namely union, overlay and repository) into
 * account to sync the changes back into the repository.
 *
 * On the repository side we have a catalogs directory that mimicks the
 * shadow directory structure and stores compressed and uncompressed
 * versions of all catalogs.  The raw data are stored in the data
 * subdirectory in zlib-compressed form.  They are named with their SHA-1
 * hash of the compressed file (like in CVMFS client cache, but with a
 * 2-level cache hierarchy).  Symlinks from the catalog directory to the
 * data directory form the connection. If necessary, add a .htaccess file
 * to allow Apache to follow the symlinks.
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "swissknife_sync.h"

#include <fcntl.h>

#include <cstdlib>
#include <cstdio>

#include <string>
#include <vector>

#include "platform.h"
#include "sync_union.h"
#include "sync_mediator.h"
#include "catalog_mgr_rw.h"
#include "util.h"
#include "logging.h"
#include "download.h"
#include "manifest.h"

using namespace std;  // NOLINT


bool swissknife::CommandSync::CheckParams(const SyncParameters &p) {
  if (!DirectoryExists(p.dir_scratch)) {
    PrintError("overlay (copy on write) directory does not exist");
    return false;
  }
  if (!DirectoryExists(p.dir_union)) {
    PrintError("union volume does not exist");
    return false;
  }
  if (!DirectoryExists(p.dir_rdonly)) {
    PrintError("cvmfs read/only repository does not exist");
    return false;
  }
  if (p.stratum0 == "") {
    PrintError("Stratum0 url missing");
    return false;
  }

  if (p.manifest_path == "") {
    PrintError("manifest output required");
    return false;
  }
  if (!DirectoryExists(p.dir_temp)) {
    PrintError("data store directory does not exist");
    return false;
  }

  if (p.min_file_chunk_size >= p.avg_file_chunk_size ||
      p.avg_file_chunk_size >= p.max_file_chunk_size) {
    PrintError("file chunk size values are not sane");
    return false;
  }

  if (p.catalog_entry_warn_threshold <= 10000) {
    PrintError("catalog entry warning threshold is too low "
               "(should be at least 10000)");
    return false;
  }

  return true;
}


int swissknife::CommandCreate::Main(const swissknife::ArgumentList &args) {
  const string manifest_path = *args.find('o')->second;
  const string dir_temp = *args.find('t')->second;
  const string spooler_definition = *args.find('r')->second;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('a') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('a')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }
  bool volatile_content = false;
  if (args.find('v') != args.end())
    volatile_content = true;

  const upload::SpoolerDefinition sd(spooler_definition, hash_algorithm);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);

  // TODO: consider using the unique pointer to come in Github Pull Request 46
  manifest::Manifest *manifest =
    catalog::WritableCatalogManager::CreateRepository(
      dir_temp, volatile_content, spooler);
  if (!manifest) {
    PrintError("Failed to create new repository");
    return 1;
  }

  spooler->WaitForUpload();
  delete spooler;

  if (!manifest->Export(manifest_path)) {
    PrintError("Failed to create new repository");
    delete manifest;
    return 5;
  }
  delete manifest;

  return 0;
}


int swissknife::CommandUpload::Main(const swissknife::ArgumentList &args) {
  const string source = *args.find('i')->second;
  const string dest = *args.find('o')->second;
  const string spooler_definition = *args.find('r')->second;
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('a') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('a')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }

  const upload::SpoolerDefinition sd(spooler_definition, hash_algorithm);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);
  spooler->Upload(source, dest);
  spooler->WaitForUpload();

  if (spooler->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to upload %s", source.c_str());
    return 1;
  }

  delete spooler;

  return 0;
}


int swissknife::CommandPeek::Main(const swissknife::ArgumentList &args) {
  const string file_to_peek = *args.find('d')->second;
  const string spooler_definition = *args.find('r')->second;

  // Hash doesn't matter
  const upload::SpoolerDefinition sd(spooler_definition, shash::kAny);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);
  const bool success = spooler->Peek(file_to_peek);

  if (spooler->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to peek for %s",
             file_to_peek.c_str());
    return 2;
  }
  if (!success) {
    LogCvmfs(kLogCatalog, kLogStdout, "%s not found", file_to_peek.c_str());
    return 1;
  }
  LogCvmfs(kLogCatalog, kLogStdout, "%s available", file_to_peek.c_str());

  delete spooler;

  return 0;
}


int swissknife::CommandRemove::Main(const ArgumentList &args) {
  const string file_to_delete     = *args.find('o')->second;
  const string spooler_definition = *args.find('r')->second;

  // Hash doesn't matter
  const upload::SpoolerDefinition sd(spooler_definition, shash::kAny);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);
  const bool success = spooler->Remove(file_to_delete);

  if (spooler->GetNumberOfErrors() > 0 || ! success) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to delete %s",
             file_to_delete.c_str());
    return 1;
  }

  delete spooler;

  return 0;
}


int swissknife::CommandApplyDirtab::Main(const ArgumentList &args) {
  const string master_keys   = *args.find('k')->second;
  const string trusted_certs = (args.find('y') != args.end()) ?
                                  *args.find('y')->second : "";
  const string dirtab_file   = *args.find('d')->second;
  const string base_hash     = *args.find('b')->second;
  const string stratum0      = *args.find('w')->second;
  const string dir_temp      = *args.find('t')->second;

  return 0;
}


struct chunk_arg {
  chunk_arg(char param, size_t *save_to) : param(param), save_to(save_to) {}
  char    param;
  size_t *save_to;
};

bool swissknife::CommandSync::ReadFileChunkingArgs(
                                          const swissknife::ArgumentList &args,
                                          SyncParameters &params) {
  typedef std::vector<chunk_arg> ChunkArgs;

  // define where to store the value of which file chunk argument
  ChunkArgs chunk_args;
  chunk_args.push_back(chunk_arg('a', &params.avg_file_chunk_size));
  chunk_args.push_back(chunk_arg('l', &params.min_file_chunk_size));
  chunk_args.push_back(chunk_arg('h', &params.max_file_chunk_size));

  // read the arguments
  ChunkArgs::const_iterator i    = chunk_args.begin();
  ChunkArgs::const_iterator iend = chunk_args.end();
  for (; i != iend; ++i) {
    swissknife::ArgumentList::const_iterator arg = args.find(i->param);

    if (arg != args.end()) {
      size_t arg_value = static_cast<size_t>(String2Uint64(*arg->second));
      if (arg_value > 0) *i->save_to = arg_value;
      else return false;
    }
  }

  // check if argument values are sane
  return true;
}


int swissknife::CommandSync::Main(const swissknife::ArgumentList &args) {
  SyncParameters params;

  // Initialization
  params.dir_union = MakeCanonicalPath(*args.find('u')->second);
  params.dir_scratch = MakeCanonicalPath(*args.find('s')->second);
  params.dir_rdonly = MakeCanonicalPath(*args.find('c')->second);
  params.dir_temp = MakeCanonicalPath(*args.find('t')->second);
  params.base_hash = *args.find('b')->second;
  params.stratum0 = *args.find('w')->second;
  params.manifest_path = *args.find('o')->second;
  params.spooler_definition = *args.find('r')->second;

  if (args.find('f') != args.end())
    params.union_fs_type = *args.find('f')->second;
  if (args.find('x') != args.end()) params.print_changeset = true;
  if (args.find('y') != args.end()) params.dry_run = true;
  if (args.find('m') != args.end()) params.mucatalogs = true;
  if (args.find('i') != args.end()) params.ignore_xdir_hardlinks = true;
  if (args.find('d') != args.end()) params.stop_for_catalog_tweaks = true;
  if (args.find('z') != args.end()) {
    unsigned log_level =
    1 << (kLogLevel0 + String2Uint64(*args.find('z')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }

  if (args.find('p') != args.end()) {
    params.use_file_chunking = true;
    if (!ReadFileChunkingArgs(args, params)) {
      PrintError("Failed to read file chunk size values");
      return 2;
    }
  }
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('e') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('e')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }

  if (args.find('j') != args.end()) {
    params.catalog_entry_warn_threshold = String2Uint64(*args.find('j')->second);
  }

  if (!CheckParams(params)) return 2;

  // Start spooler
  const upload::SpoolerDefinition spooler_definition(
    params.spooler_definition,
    hash_algorithm,
    params.use_file_chunking,
    params.min_file_chunk_size,
    params.avg_file_chunk_size,
    params.max_file_chunk_size);
  params.spooler = upload::Spooler::Construct(spooler_definition);
  if (NULL == params.spooler)
    return 3;

  g_download_manager->Init(1, true);

  catalog::WritableCatalogManager
    catalog_manager(shash::MkFromHexPtr(shash::HexPtr(params.base_hash)),
                    params.stratum0, params.dir_temp, params.spooler,
                    g_download_manager, params.catalog_entry_warn_threshold);
  catalog_manager.Init();
  publish::SyncMediator mediator(&catalog_manager, &params);
  publish::SyncUnion *sync;
  if (params.union_fs_type == "overlayfs") {
    sync = new publish::SyncUnionOverlayfs(&mediator,
                                           params.dir_rdonly,
                                           params.dir_union,
                                           params.dir_scratch);
  } else if (params.union_fs_type == "aufs") {
    sync = new publish::SyncUnionAufs(&mediator,
                                      params.dir_rdonly,
                                      params.dir_union,
                                      params.dir_scratch);
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "unknown union file system: %s",
             params.union_fs_type.c_str());
    return 3;
  }

  sync->Traverse();
  // TODO: consider using the unique pointer to come in Github Pull Request 46
  manifest::Manifest *manifest = mediator.Commit();

  g_download_manager->Fini();

  LogCvmfs(kLogCvmfs, kLogStdout, "Exporting repository manifest");
  if (!manifest) {
    PrintError("something went wrong during sync");
    return 4;
  }

  // finalize the spooler
  params.spooler->WaitForUpload();
  delete params.spooler;

  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    delete manifest;
    return 5;
  }
  delete manifest;

  return 0;
}
