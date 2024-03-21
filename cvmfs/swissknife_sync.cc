/**
 * This file is part of the CernVM File System
 *
 * This tool figures out the changes made to a cvmfs repository by means
 * of a union file system mounted on top of a cvmfs volume.
 * We take all three volumes (namely union, overlay and repository) into
 * account to sync the changes back into the repository.
 *
 * On the repository side we have a catalogs directory that mimics the
 * shadow directory structure and stores compressed and uncompressed
 * versions of all catalogs.  The raw data are stored in the data
 * subdirectory in zlib-compressed form.  They are named with their SHA-1
 * hash of the compressed file (like in CVMFS client cache, but with a
 * 2-level cache hierarchy).  Symlinks from the catalog directory to the
 * data directory form the connection. If necessary, add a .htaccess file
 * to allow Apache to follow the symlinks.
 */

// NOLINTNEXTLINE
#define _FILE_OFFSET_BITS 64
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS

#include "swissknife_sync.h"
#include "cvmfs_config.h"

#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/capability.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "catalog_virtual.h"
#include "manifest.h"
#include "monitor.h"
#include "network/download.h"
#include "path_filters/dirtab.h"
#include "reflog.h"
#include "sanitizer.h"
#include "statistics.h"
#include "statistics_database.h"
#include "swissknife_capabilities.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "sync_union_aufs.h"
#include "sync_union_overlayfs.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/string.h"

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

  if (HasPrefix(p.spooler_definition, "gw", false)) {
    if (p.session_token_file.empty()) {
      PrintError(
          "Session token file has to be provided "
          "when upstream type is gw.");
      return false;
    }
  }

  return true;
}

int swissknife::CommandCreate::Main(const swissknife::ArgumentList &args) {
  const string manifest_path = *args.find('o')->second;
  const string dir_temp = *args.find('t')->second;
  const string spooler_definition = *args.find('r')->second;
  const string repo_name = *args.find('n')->second;
  const string reflog_chksum_path = *args.find('R')->second;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      kLogLevel0 << String2Uint64(*args.find('l')->second);
    if (log_level > kLogNone) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid log level");
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

  const bool volatile_content = (args.count('v') > 0);
  const bool garbage_collectable = (args.count('z') > 0);
  std::string voms_authz;
  if (args.find('V') != args.end()) {
    voms_authz = *args.find('V')->second;
  }

  const upload::SpoolerDefinition sd(spooler_definition, hash_algorithm,
                                     zlib::kZlibDefault);
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(sd));
  assert(spooler.IsValid());

  UniquePtr<manifest::Manifest> manifest(
      catalog::WritableCatalogManager::CreateRepository(
          dir_temp, volatile_content, voms_authz, spooler.weak_ref()));
  if (!manifest.IsValid()) {
    PrintError("Failed to create new repository");
    return 1;
  }

  UniquePtr<manifest::Reflog> reflog(CreateEmptyReflog(dir_temp, repo_name));
  if (!reflog.IsValid()) {
    PrintError("Failed to create fresh Reflog");
    return 1;
  }

  reflog->DropDatabaseFileOwnership();
  string reflog_path = reflog->database_file();
  reflog.Destroy();
  shash::Any reflog_hash(hash_algorithm);
  manifest::Reflog::HashDatabase(reflog_path, &reflog_hash);
  spooler->UploadReflog(reflog_path);
  spooler->WaitForUpload();
  unlink(reflog_path.c_str());
  if (spooler->GetNumberOfErrors()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload reflog");
    return 4;
  }
  assert(!reflog_chksum_path.empty());
  manifest::Reflog::WriteChecksum(reflog_chksum_path, reflog_hash);

  // set optional manifest fields
  const bool needs_bootstrap_shortcuts = !voms_authz.empty();
  manifest->set_garbage_collectability(garbage_collectable);
  manifest->set_has_alt_catalog_path(needs_bootstrap_shortcuts);

  if (!manifest->Export(manifest_path)) {
    PrintError("Failed to create new repository");
    return 5;
  }

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
  const string file_to_delete = *args.find('o')->second;
  const string spooler_definition = *args.find('r')->second;

  // Hash doesn't matter
  const upload::SpoolerDefinition sd(spooler_definition, shash::kAny);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);
  spooler->RemoveAsync(file_to_delete);
  spooler->WaitForUpload();

  if (spooler->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to delete %s",
             file_to_delete.c_str());
    return 1;
  }

  delete spooler;

  return 0;
}

int swissknife::CommandApplyDirtab::Main(const ArgumentList &args) {
  const string dirtab_file = *args.find('d')->second;
  union_dir_ = MakeCanonicalPath(*args.find('u')->second);
  scratch_dir_ = MakeCanonicalPath(*args.find('s')->second);
  const shash::Any base_hash = shash::MkFromHexPtr(
      shash::HexPtr(*args.find('b')->second), shash::kSuffixCatalog);
  const string stratum0 = *args.find('w')->second;
  const string dir_temp = *args.find('t')->second;
  verbose_ = (args.find('x') != args.end());

  // check if there is a dirtab file
  if (!FileExists(dirtab_file)) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg,
             "Didn't find a dirtab at '%s'. Skipping...", dirtab_file.c_str());
    return 0;
  }

  // parse dirtab file
  catalog::Dirtab *dirtab = catalog::Dirtab::Create(dirtab_file);
  if (!dirtab->IsValid()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Invalid or not readable dirtab '%s'",
             dirtab_file.c_str());
    return 1;
  }
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "Found %lu rules in dirtab '%s'",
           dirtab->RuleCount(), dirtab_file.c_str());

  // initialize catalog infrastructure
  const bool auto_manage_catalog_files = true;
  const bool follow_redirects = (args.count('L') > 0);
  const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
  if (!InitDownloadManager(follow_redirects, proxy)) {
    return 1;
  }
  catalog::SimpleCatalogManager catalog_manager(
      base_hash, stratum0, dir_temp, download_manager(), statistics(),
      auto_manage_catalog_files);
  catalog_manager.Init();

  vector<string> new_nested_catalogs;
  DetermineNestedCatalogCandidates(*dirtab, &catalog_manager,
                                   &new_nested_catalogs);
  const bool success = CreateCatalogMarkers(new_nested_catalogs);
  delete dirtab;

  return (success) ? 0 : 1;
}


namespace {

// Overwrite directory traversal in the globbing in order to avoid breaking out
// the repository tree

std::string *g_glob_uniondir = NULL;

bool GlobCheckPath(const char *name) {
  char resolved_cstr[PATH_MAX];
  char *retval = realpath(name, resolved_cstr);
  if (retval == NULL) return false;

  std::string resolved(resolved_cstr);
  if (resolved == *g_glob_uniondir) return true;
  if (!HasPrefix(resolved, (*g_glob_uniondir) + "/", false /*ignore_case*/)) {
    errno = EACCES;
    return false;
  }
  return true;
}

void *GlobOpendir(const char *name) {
  if (!GlobCheckPath(name)) return NULL;
  return opendir(name);
}

void GlobClosedir(void *dirp) {
  closedir(static_cast<DIR *>(dirp));
}

struct dirent *GlobReaddir(void *dirp) {
  return readdir(static_cast<DIR *>(dirp));
}

int GlobLstat(const char *name, struct stat *st) {
  if (!GlobCheckPath(name)) return -1;
  return lstat(name, st);
}

int GlobStat(const char *name, struct stat *st) {
  if (!GlobCheckPath(name)) return -1;
  return stat(name, st);
}


}  // anonymous namespace

void swissknife::CommandApplyDirtab::DetermineNestedCatalogCandidates(
    const catalog::Dirtab &dirtab,
    catalog::SimpleCatalogManager *catalog_manager,
    vector<string> *nested_catalog_candidates) {
  // find possible new nested catalog locations
  const catalog::Dirtab::Rules &lookup_rules = dirtab.positive_rules();
  catalog::Dirtab::Rules::const_iterator i = lookup_rules.begin();
  const catalog::Dirtab::Rules::const_iterator iend = lookup_rules.end();
  for (; i != iend; ++i) {
    assert(!i->is_negation);

    // run a glob using the current dirtab rule on the current repository
    // state
    const std::string &glob_string = i->pathspec.GetGlobString();
    const std::string &glob_string_abs = union_dir_ + glob_string;
    const int glob_flags = GLOB_ONLYDIR | GLOB_NOSORT | GLOB_PERIOD |
                           GLOB_ALTDIRFUNC;
    glob_t glob_res;
    g_glob_uniondir = new std::string(union_dir_);
    glob_res.gl_opendir = GlobOpendir;
    glob_res.gl_readdir = GlobReaddir;
    glob_res.gl_closedir = GlobClosedir;
    glob_res.gl_lstat = GlobLstat;
    glob_res.gl_stat = GlobStat;
    const int glob_retval =
        glob(glob_string_abs.c_str(), glob_flags, NULL, &glob_res);
    delete g_glob_uniondir;
    g_glob_uniondir = NULL;

    if (glob_retval == 0) {
      // found some candidates... filtering by cvmfs catalog structure
      LogCvmfs(kLogCatalog, kLogDebug, "Found %lu entries for pathspec (%s)",
               glob_res.gl_pathc, glob_string.c_str());
      FilterCandidatesFromGlobResult(dirtab, glob_res.gl_pathv,
                                     glob_res.gl_pathc, catalog_manager,
                                     nested_catalog_candidates);
    } else if (glob_retval == GLOB_NOMATCH) {
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
    const catalog::Dirtab &dirtab, char **paths, const size_t npaths,
    catalog::SimpleCatalogManager *catalog_manager,
    std::vector<std::string> *nested_catalog_candidates) {
  // go through the paths produced by glob() and filter them
  for (size_t i = 0; i < npaths; ++i) {
    // process candidate paths
    const std::string candidate(paths[i]);
    const std::string candidate_rel = candidate.substr(union_dir_.size());

    // check if path points to a directory
    platform_stat64 candidate_info;
    const int lstat_retval = platform_lstat(candidate.c_str(), &candidate_info);
    if (lstat_retval != 0) {
      LogCvmfs(kLogCatalog, kLogDebug | kLogStderr | kLogSyslogErr,
               "Error in processing .cvmfsdirtab: cannot access %s (%d)",
               candidate.c_str(), errno);
      abort();
    }
    assert(lstat_retval == 0);
    if (!S_ISDIR(candidate_info.st_mode)) {
      // The GLOB_ONLYDIR flag is only a hint, non-directories can still be
      // returned
      LogCvmfs(kLogCatalog, kLogDebug,
               "The '%s' dirtab entry does not point to a directory "
               "but to a file or a symbolic link",
               candidate_rel.c_str());
      continue;
    }

    // check if the path is a meta-directory (. or ..)
    assert(candidate_rel.size() >= 2);
    if (candidate_rel.substr(candidate_rel.size() - 2) == "/." ||
        candidate_rel.substr(candidate_rel.size() - 3) == "/..") {
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
    const bool lookup_success = catalog_manager->LookupPath(
        candidate_rel, catalog::kLookupDefault, &dirent);
    if (!lookup_success) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "Didn't find '%s' in catalogs, could "
               "be a new directory and nested catalog.",
               candidate_rel.c_str());
      nested_catalog_candidates->push_back(candidate);
    } else if (!dirent.IsNestedCatalogMountpoint() &&
               !dirent.IsNestedCatalogRoot()) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "Found '%s' in catalogs but is not a "
               "nested catalog yet.",
               candidate_rel.c_str());
      nested_catalog_candidates->push_back(candidate);
    } else {
      // check if the nested catalog marker is still there, we might need to
      // recreate the catalog after manual marker removal
      // Note: First we check if the parent directory shows up in the scratch
      //       space to verify that it was touched (copy-on-write)
      //       Otherwise we would force the cvmfs client behind the union
      //       file-
      //       system to (potentially) unnecessarily fetch catalogs
      if (DirectoryExists(scratch_dir_ + candidate_rel) &&
          !FileExists(union_dir_ + candidate_rel + "/.cvmfscatalog")) {
        LogCvmfs(kLogCatalog, kLogStdout,
                 "WARNING: '%s' should be a nested "
                 "catalog according to the dirtab. "
                 "Recreating...",
                 candidate_rel.c_str());
        nested_catalog_candidates->push_back(candidate);
      } else {
        LogCvmfs(kLogCatalog, kLogDebug,
                 "Found '%s' in catalogs and it already is a nested catalog.",
                 candidate_rel.c_str());
      }
    }
  }
}

bool swissknife::CommandApplyDirtab::CreateCatalogMarkers(
    const std::vector<std::string> &new_nested_catalogs) {
  // go through the new nested catalog paths and create .cvmfscatalog markers
  // where necessary
  bool success = true;
  std::vector<std::string>::const_iterator k = new_nested_catalogs.begin();
  const std::vector<std::string>::const_iterator kend =
      new_nested_catalogs.end();
  for (; k != kend; ++k) {
    assert(!k->empty() && k->size() > union_dir_.size());

    // was the marker already created by hand?
    const std::string marker_path = *k + "/.cvmfscatalog";
    if (FileExists(marker_path)) {
      continue;
    }

    // create a nested catalog marker
    const mode_t mode = kDefaultFileMode;
    const int fd = open(marker_path.c_str(), O_CREAT, mode);
    if (fd < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to create nested catalog marker "
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
  char param;
  size_t *save_to;
};

bool swissknife::CommandSync::ReadFileChunkingArgs(
    const swissknife::ArgumentList &args, SyncParameters *params) {
  typedef std::vector<chunk_arg> ChunkArgs;

  // define where to store the value of which file chunk argument
  ChunkArgs chunk_args;
  chunk_args.push_back(chunk_arg('a', &params->avg_file_chunk_size));
  chunk_args.push_back(chunk_arg('l', &params->min_file_chunk_size));
  chunk_args.push_back(chunk_arg('h', &params->max_file_chunk_size));

  // read the arguments
  ChunkArgs::const_iterator i = chunk_args.begin();
  ChunkArgs::const_iterator iend = chunk_args.end();
  for (; i != iend; ++i) {
    swissknife::ArgumentList::const_iterator arg = args.find(i->param);

    if (arg != args.end()) {
      size_t arg_value = static_cast<size_t>(String2Uint64(*arg->second));
      if (arg_value > 0) {
        *i->save_to = arg_value;
      } else {
        return false;
      }
    }
  }

  // check if argument values are sane
  return true;
}

int swissknife::CommandSync::Main(const swissknife::ArgumentList &args) {
  string start_time = GetGMTimestamp();

  // Spawn monitoring process (watchdog)
  std::string watchdog_dir = "/tmp";
  char watchdog_path[PATH_MAX];
  std::string timestamp = GetGMTimestamp("%Y.%m.%d-%H.%M.%S");
  int path_size = snprintf(watchdog_path, sizeof(watchdog_path),
                           "%s/cvmfs-swissknife-sync-stacktrace.%s.%d",
                           watchdog_dir.c_str(), timestamp.c_str(), getpid());
  assert(path_size > 0);
  assert(path_size < PATH_MAX);
  UniquePtr<Watchdog> watchdog(Watchdog::Create(NULL));
  watchdog->Spawn(std::string(watchdog_path));

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

  params.ttl_seconds = catalog::Catalog::kDefaultTTL;

  if (args.find('f') != args.end())
    params.union_fs_type = *args.find('f')->second;
  if (args.find('A') != args.end()) params.is_balanced = true;
  if (args.find('x') != args.end()) params.print_changeset = true;
  if (args.find('y') != args.end()) params.dry_run = true;
  if (args.find('m') != args.end()) params.mucatalogs = true;
  if (args.find('i') != args.end()) params.ignore_xdir_hardlinks = true;
  if (args.find('d') != args.end()) params.stop_for_catalog_tweaks = true;
  if (args.find('V') != args.end()) params.voms_authz = true;
  if (args.find('F') != args.end()) params.authz_file = *args.find('F')->second;
  if (args.find('k') != args.end()) params.include_xattrs = true;
  if (args.find('Y') != args.end()) params.external_data = true;
  if (args.find('W') != args.end()) params.direct_io = true;
  if (args.find('S') != args.end()) {
    bool retval = catalog::VirtualCatalog::ParseActions(
        *args.find('S')->second, &params.virtual_dir_actions);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid virtual catalog options: %s",
               args.find('S')->second->c_str());
      return 1;
    }
  }
  if (args.find('z') != args.end()) {
    unsigned log_level =
        1 << (kLogLevel0 + String2Uint64(*args.find('z')->second));
    if (log_level > kLogNone) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid log level");
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }

  if (args.find('X') != args.end())
    params.max_weight = String2Uint64(*args.find('X')->second);
  if (args.find('M') != args.end())
    params.min_weight = String2Uint64(*args.find('M')->second);

  if (args.find('p') != args.end()) {
    params.use_file_chunking = true;
    if (!ReadFileChunkingArgs(args, &params)) {
      PrintError("Failed to read file chunk size values");
      return 2;
    }
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

  if (args.find('E') != args.end()) params.enforce_limits = true;
  if (args.find('Q') != args.end()) {
    params.nested_kcatalog_limit = String2Uint64(*args.find('Q')->second);
  } else {
    params.nested_kcatalog_limit = SyncParameters::kDefaultNestedKcatalogLimit;
  }
  if (args.find('R') != args.end()) {
    params.root_kcatalog_limit = String2Uint64(*args.find('R')->second);
  } else {
    params.root_kcatalog_limit = SyncParameters::kDefaultRootKcatalogLimit;
  }
  if (args.find('U') != args.end()) {
    params.file_mbyte_limit = String2Uint64(*args.find('U')->second);
  } else {
    params.file_mbyte_limit = SyncParameters::kDefaultFileMbyteLimit;
  }

  if (args.find('v') != args.end()) {
    sanitizer::IntegerSanitizer sanitizer;
    if (!sanitizer.IsValid(*args.find('v')->second)) {
      PrintError("invalid revision number");
      return 1;
    }
    params.manual_revision = String2Uint64(*args.find('v')->second);
  }

  params.branched_catalog = args.find('B') != args.end();

  if (args.find('q') != args.end()) {
    params.max_concurrent_write_jobs = String2Uint64(*args.find('q')->second);
  }

  if (args.find('0') != args.end()) {
    params.num_upload_tasks = String2Uint64(*args.find('0')->second);
  }

  if (args.find('T') != args.end()) {
    params.ttl_seconds = String2Uint64(*args.find('T')->second);
  }

  if (args.find('g') != args.end()) {
    params.ignore_special_files = true;
  }

  if (args.find('P') != args.end()) {
    params.session_token_file = *args.find('P')->second;
  }

  if (args.find('H') != args.end()) {
    params.key_file = *args.find('H')->second;
  }

  if (args.find('D') != args.end()) {
    params.repo_tag.SetName(*args.find('D')->second);
  }

  if (args.find('J') != args.end()) {
    params.repo_tag.SetDescription(*args.find('J')->second);
  }

  const bool upload_statsdb = (args.count('I') > 0);

  if (!CheckParams(params)) return 2;
  // This may fail, in which case a warning is printed and the process continues
  ObtainDacReadSearchCapability();

  perf::StatisticsTemplate publish_statistics("publish", this->statistics());

  // Start spooler
  upload::SpoolerDefinition spooler_definition(
      params.spooler_definition, hash_algorithm, params.compression_alg,
      params.generate_legacy_bulk_chunks, params.use_file_chunking,
      params.min_file_chunk_size, params.avg_file_chunk_size,
      params.max_file_chunk_size, params.session_token_file, params.key_file);
  if (params.max_concurrent_write_jobs > 0) {
    spooler_definition.number_of_concurrent_uploads =
        params.max_concurrent_write_jobs;
  }
  spooler_definition.num_upload_tasks = params.num_upload_tasks;

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

  /*
   * Note: If the upstream is of type gateway, due to the possibility of
   * concurrent release managers, it's possible to have a different local and
   * remote root hashes. We proceed by loading the remote manifest but we give
   * an empty base hash.
   */
  UniquePtr<manifest::Manifest> manifest;
  if (params.branched_catalog) {
    // Throw-away manifest
    manifest = new manifest::Manifest(shash::Any(), 0, "");
  } else if (params.virtual_dir_actions !=
             catalog::VirtualCatalog::kActionNone) {
    manifest = this->OpenLocalManifest(params.manifest_path);
    params.base_hash = manifest->catalog_hash();
  } else {
    // TODO(jblomer): revert to params.base_hash if spooler driver type is not
    // upload::SpoolerDefinition::Gateway
    manifest =
      FetchRemoteManifest(params.stratum0, params.repo_name, shash::Any());
  }
  if (!manifest.IsValid()) {
    return 3;
  }

  StatisticsDatabase *stats_db =
    StatisticsDatabase::OpenStandardDB(params.repo_name);

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

  // Should be before the synchronization starts to avoid race of GetTTL with
  // other sqlite operations
  if ((params.ttl_seconds > 0) &&
      ((params.ttl_seconds != catalog_manager.GetTTL()) ||
       !catalog_manager.HasExplicitTTL())) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Setting repository TTL to %" PRIu64 "s",
             params.ttl_seconds);
    catalog_manager.SetTTL(params.ttl_seconds);
  }

  // Either real catalogs or virtual catalog
  if (params.virtual_dir_actions == catalog::VirtualCatalog::kActionNone) {
    publish::SyncUnion *sync;
    if (params.union_fs_type == "overlayfs") {
      sync = new publish::SyncUnionOverlayfs(
          &mediator, params.dir_rdonly, params.dir_union, params.dir_scratch);
    } else if (params.union_fs_type == "aufs") {
      sync = new publish::SyncUnionAufs(&mediator, params.dir_rdonly,
                                        params.dir_union, params.dir_scratch);
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown union file system: %s",
               params.union_fs_type.c_str());
      return 3;
    }

    if (!sync->Initialize()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Initialization of the synchronisation "
               "engine failed");
      return 4;
    }

    sync->Traverse();
  } else {
    assert(!manifest->history().IsNull());
    catalog::VirtualCatalog virtual_catalog(
        manifest.weak_ref(), download_manager(), &catalog_manager, &params);
    virtual_catalog.Generate(params.virtual_dir_actions);
  }

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
    if (!params.dry_run) {
      stats_db->StorePublishStatistics(this->statistics(), start_time, false);
      if (upload_statsdb) {
        stats_db->UploadStatistics(params.spooler);
      }
    }
    return 5;
  }

  perf::Counter *revision_counter = statistics()->Register("publish.revision",
                                                  "Published revision number");
  revision_counter->Set(static_cast<int64_t>(
    catalog_manager.GetRootCatalog()->revision()));

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
    PrintError("Failed to commit transaction.");
    if (!params.dry_run) {
      stats_db->StorePublishStatistics(this->statistics(), start_time, false);
      if (upload_statsdb) {
        stats_db->UploadStatistics(params.spooler);
      }
    }
    return 9;
  }

  if (!params.dry_run) {
    stats_db->StorePublishStatistics(this->statistics(), start_time, true);
    if (upload_statsdb) {
      stats_db->UploadStatistics(params.spooler);
    }
  }

  delete params.spooler;

  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    return 6;
  }

  return 0;
}
