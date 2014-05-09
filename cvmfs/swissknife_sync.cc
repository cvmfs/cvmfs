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
#include <glob.h>

#include <string>
#include <vector>

#include "platform.h"
#include "sync_union.h"
#include "sync_mediator.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "dirtab.h"
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
  const string dirtab_file   = *args.find('d')->second;
  union_dir_                 = MakeCanonicalPath(*args.find('u')->second);
  const string base_hash_str = *args.find('b')->second;
  const string stratum0      = *args.find('w')->second;
  const string dir_temp      = *args.find('t')->second;
  verbose_                   = (args.find('x') != args.end());

  // check if there is a dirtab file
  if (! FileExists(dirtab_file)) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg, "Didn't find a dirtab at '%s'. "
                                          "Skipping...",
             dirtab_file.c_str());
    return 0;
  }

  // parse dirtab file
  catalog::Dirtab dirtab(dirtab_file);
  if (! dirtab.IsValid()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Invalid or not readable dirtab '%s'",
             dirtab_file.c_str());
    return 1;
  }
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "Found %d rules in dirtab '%s'",
           dirtab.RuleCount(), dirtab_file.c_str());

  // initialize catalog infrastructure
  g_download_manager->Init(1, true);
  const shash::Any base_hash = shash::MkFromHexPtr(shash::HexPtr(base_hash_str));
  catalog::SimpleCatalogManager catalog_manager(base_hash,
                                                stratum0,
                                                dir_temp,
                                                g_download_manager);
  catalog_manager.Init();

  std::vector<std::string> new_nested_catalogs;
  DetermineNestedCatalogCandidates(dirtab, catalog_manager,
                                   new_nested_catalogs);
  const bool success = CreateCatalogMarkers(new_nested_catalogs);

  return (success) ? 0 : 1;
}



void swissknife::CommandApplyDirtab::DetermineNestedCatalogCandidates(
                    const catalog::Dirtab          &dirtab,
                    catalog::SimpleCatalogManager  &catalog_manager,
                    std::vector<std::string>       &nested_catalog_candidates) {
  // find possible new nested catalog locations
  const catalog::Dirtab::Rules &lookup_rules = dirtab.positive_rules();
        catalog::Dirtab::Rules::const_iterator i    = lookup_rules.begin();
  const catalog::Dirtab::Rules::const_iterator iend = lookup_rules.end();
  for (; i != iend; ++i) {
    assert (! i->is_negation);

    // run a glob using the current dirtab rule on the current repository state
    const std::string &glob_string = i->pathspec.GetGlobString();
    const std::string &glob_string_abs = union_dir_ + glob_string;
    const int glob_flags  = GLOB_ONLYDIR | GLOB_NOSORT | GLOB_PERIOD;
    glob_t    glob_res;
    const int glob_retval = glob(glob_string_abs.c_str(), glob_flags,
                                 NULL, &glob_res);

    if (glob_retval == 0) {
      // found some candidates... filtering by cvmfs catalog structure
      LogCvmfs(kLogCatalog, kLogDebug, "Found %d entries for pathspec (%s)",
                                       glob_res.gl_pathc, glob_string.c_str());
      FilterCandidatesFromGlobResult(dirtab,
                                     glob_res.gl_pathv, glob_res.gl_pathc,
                                     catalog_manager,
                                     nested_catalog_candidates);
    }
    else if (glob_retval == GLOB_NOMATCH) {
      LogCvmfs(kLogCvmfs, kLogStderr, "WARNING: cannot apply pathspec %s",
                                      glob_string.c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to run glob matching (%s)",
                                      glob_string.c_str());
    }

    globfree(&glob_res);
  }
}


void swissknife::CommandApplyDirtab::FilterCandidatesFromGlobResult(
                    const catalog::Dirtab &dirtab,
                    char **paths, const size_t npaths,
                    catalog::SimpleCatalogManager  &catalog_manager,
                    std::vector<std::string>       &nested_catalog_candidates) {
  // go through the paths produced by glob() and filter them
  for (size_t i = 0; i < npaths; ++i) {
    // process candidate paths
    const std::string candidate(paths[i]);
    const std::string candidate_rel = candidate.substr(union_dir_.size());

    // check if path points to a directory
    platform_stat64 candidate_info;
    const int lstat_retval = platform_lstat(candidate.c_str(), &candidate_info);
    assert (lstat_retval == 0);
    if (! S_ISDIR(candidate_info.st_mode)) {
      continue;
    }

    // check if the path is a meta-directory (. or ..)
    if (candidate_rel.substr(candidate_rel.size() - 3) == "/.." ||
        candidate_rel.substr(candidate_rel.size() - 2) == "/.") {
      continue;
    }

    // check that the path isn't excluded in the dirtab
    if (dirtab.IsOpposing(candidate_rel)) {
      LogCvmfs(kLogCatalog, kLogDebug, "Candidate '%s' is excluded by dirtab",
              candidate_rel.c_str());
      continue;
    }

    // lookup the path in the catalog structure to find out if it already
    // points to a nested catalog transition point. Furthermore it could be
    // a new directory and thus not in any catalog yet.
    catalog::DirectoryEntry dirent;
    const bool lookup_success = catalog_manager.LookupPath(
                                                      candidate_rel,
                                                      catalog::kLookupSole,
                                                      &dirent);
    if (! lookup_success) {
      LogCvmfs(kLogCatalog, kLogDebug, "Didn't find '%s' in catalogs, could "
                                       "be a new directory and nested catalog.",
                                       candidate_rel.c_str());
      nested_catalog_candidates.push_back(candidate);
    } else if (! dirent.IsNestedCatalogMountpoint() &&
               ! dirent.IsNestedCatalogRoot()) {
      LogCvmfs(kLogCatalog, kLogDebug, "Found '%s' in catalogs but is not a "
                                       "nested catalog yet.",
                                       candidate_rel.c_str());
      nested_catalog_candidates.push_back(candidate);
    } else {
      LogCvmfs(kLogCatalog, kLogDebug, "Found '%s' in catalogs and it already "
                                       "is a nested catalog.",
                                       candidate_rel.c_str());
    }
  }
}


bool swissknife::CommandApplyDirtab::CreateCatalogMarkers(
                          const std::vector<std::string> &new_nested_catalogs) {
  // go through the new nested catalog paths and create .cvmfscatalog markers
  // where necessary
  bool success = true;
        std::vector<std::string>::const_iterator k    = new_nested_catalogs.begin();
  const std::vector<std::string>::const_iterator kend = new_nested_catalogs.end();
  for (; k != kend; ++k) {
    assert (! k->empty() && k->size() > union_dir_.size());

    // was the marker already created by hand?
    const std::string marker_path = *k + "/.cvmfscatalog";
    if (FileExists(marker_path)) {
      continue;
    }

    // create a nested catalog marker
    const int fd = open(marker_path.c_str(), O_CREAT, 644);
    if (fd <= 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create nested catalog marker "
                                      "at '%s' (errno: %d)",
                                      marker_path.c_str(), errno);
      success = false;
      continue;
    }
    close(fd);

    // inform the user if requested
    if (verbose_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Auto-creating nested catalog in %s",
               k->c_str());
    }
  }

  return success;
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
