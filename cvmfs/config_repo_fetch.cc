/**
 * This file is part of the CernVM File System.
 */

#include "config_repo_fetch.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "cache.h"
#include "catalog_mgr_client.h"
#include "clientctx.h"
#include "directory_entry.h"
#include "fetch.h"
#include "interrupt.h"
#include "manifest.h"
#include "mountpoint.h"
#include "network/download.h"
#include "network/jobinfo.h"
#include "network/sink_mem.h"
#include "options.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

extern char **environ;

namespace config_repo {

static const char *kConfigSubtree = "/etc/cvmfs";

static const unsigned kDefaultTimeout = 10;
static const char *kCacheInstance = "cfgfetch";
// Same as FileSystem::kDefaultCacheBase
static const char *kDefaultCacheBase = "/var/lib/cvmfs";

FetchMode ParseFetchMode(const string &value) {
  const string v = ToUpper(Trim(value));
  if (v == "MOUNT")
    return kFetchMount;
  if (v == "DIRECT")
    return kFetchDirect;
  return kFetchDirectFallback;
}


static bool IsAuto(const string &proxy) {
  return ToUpper(Trim(proxy)) == "AUTO";
}

void RewriteProxies(string *proxies, string *fallback_proxies) {
  vector<string> groups;
  bool has_direct = false;
  const vector<string> raw_groups = SplitString(*proxies, ';');
  for (unsigned i = 0; i < raw_groups.size(); ++i) {
    vector<string> members = SplitString(raw_groups[i], '|');
    members.erase(remove_if(members.begin(), members.end(), IsAuto),
                  members.end());
    const string group = Trim(JoinStrings(members, "|"));
    if (group.empty())
      continue;
    if (group == "DIRECT")
      has_direct = true;
    groups.push_back(group);
  }
  has_direct = has_direct || (Trim(*fallback_proxies) == "DIRECT")
               || HasSuffix(*fallback_proxies, ";DIRECT", false);
  if (!has_direct) {
    if (Trim(*fallback_proxies).empty())
      groups.push_back("DIRECT");
    else
      *fallback_proxies += ";DIRECT";
  }
  // A proxy list without any group would fail to resolve
  if (groups.empty())
    groups.push_back("DIRECT");
  *proxies = JoinStrings(groups, ";");
}


void PrepareFetchOptions(const string &workspace, unsigned timeout_s,
                         OptionsManager *options_mgr) {
  string proxies;
  string fallback_proxies;
  options_mgr->GetValue("CVMFS_HTTP_PROXY", &proxies);
  options_mgr->GetValue("CVMFS_FALLBACK_PROXY", &fallback_proxies);
  RewriteProxies(&proxies, &fallback_proxies);
  options_mgr->SetValue("CVMFS_HTTP_PROXY", proxies);
  if (fallback_proxies.empty())
    options_mgr->UnsetValue("CVMFS_FALLBACK_PROXY");
  else
    options_mgr->SetValue("CVMFS_FALLBACK_PROXY", fallback_proxies);
  options_mgr->UnsetValue("CVMFS_PAC_URLS");
  options_mgr->SetValue("CVMFS_USE_GEOAPI", "no");

  // RAM cache: nothing is written to the local cache, and the library mode
  // supports neither the shared nor the managed posix cache
  options_mgr->UnsetValue("CVMFS_CACHE_DIR");
  options_mgr->UnsetValue("CVMFS_ALIEN_CACHE");
  options_mgr->SetValue("CVMFS_SHARED_CACHE", "no");
  options_mgr->SetValue("CVMFS_QUOTA_LIMIT", "-1");
  options_mgr->SetValue("CVMFS_STREAMING_CACHE", "no");
  options_mgr->SetValue("CVMFS_CACHE_PRIMARY", kCacheInstance);
  options_mgr->SetValue(string("CVMFS_CACHE_") + kCacheInstance + "_TYPE",
                        "ram");
  options_mgr->SetValue(string("CVMFS_CACHE_") + kCacheInstance + "_SIZE",
                        "64");
  options_mgr->SetValue("CVMFS_WORKSPACE", workspace);

  options_mgr->SetValue("CVMFS_NFS_SOURCE", "no");
  options_mgr->SetValue("CVMFS_PREFETCH_FILEBUNDLES", "no");
  options_mgr->SetValue("CVMFS_TELEMETRY_SEND", "no");
  options_mgr->UnsetValue("CVMFS_TRACEFILE");
  options_mgr->UnsetValue("CVMFS_AUTHZ_HELPER");

  string optarg;
  unsigned timeout = timeout_s;
  if (options_mgr->GetValue("CVMFS_TIMEOUT", &optarg))
    timeout = std::min(timeout, static_cast<unsigned>(String2Uint64(optarg)));
  unsigned timeout_direct = timeout_s;
  if (options_mgr->GetValue("CVMFS_TIMEOUT_DIRECT", &optarg)) {
    timeout_direct = std::min(timeout_direct,
                              static_cast<unsigned>(String2Uint64(optarg)));
  }
  options_mgr->SetValue("CVMFS_TIMEOUT", StringifyUint(std::max(timeout, 1U)));
  options_mgr->SetValue("CVMFS_TIMEOUT_DIRECT",
                        StringifyUint(std::max(timeout_direct, 1U)));
  options_mgr->SetValue("CVMFS_MAX_RETRIES", "1");
  options_mgr->SetValue("CVMFS_BACKOFF_INIT", "1");
  options_mgr->SetValue("CVMFS_BACKOFF_MAX", "1");
}


static bool ReadObject(MountPoint *mount_point, const string &path,
                       const catalog::DirectoryEntry &dirent, string *content,
                       string *error) {
  if (dirent.IsChunkedFile() || dirent.IsExternalFile()) {
    *error = path + ": chunked or external files are not supported";
    return false;
  }
  if (dirent.size() > ConfigRepoTree::kMaxFileSize) {
    *error = path + ": file too large";
    return false;
  }
  CacheManager::Label label;
  label.path = path;
  label.size = dirent.size();
  label.zip_algorithm = dirent.compression_algorithm();
  const int fd = mount_point->fetcher()->Fetch(
      CacheManager::LabeledObject(dirent.checksum(), label));
  if (fd < 0) {
    *error = path + ": fetch failed (" + StringifyInt(-fd) + ")";
    return false;
  }
  CacheManager *cache_mgr = mount_point->file_system()->cache_mgr();
  content->resize(dirent.size());
  uint64_t nbytes = 0;
  while (nbytes < dirent.size()) {
    const int64_t n = cache_mgr->Pread(fd, &(*content)[nbytes],
                                       dirent.size() - nbytes, nbytes);
    if (n <= 0)
      break;
    nbytes += n;
  }
  cache_mgr->Close(fd);
  if (nbytes != dirent.size()) {
    *error = path + ": short read";
    return false;
  }
  return true;
}


static bool WalkTree(MountPoint *mount_point, const string &dir_path,
                     uint64_t *total_size, ConfigRepoTree *tree,
                     string *error) {
  catalog::StatEntryList listing;
  PathString dir;
  dir.Assign(dir_path.data(), dir_path.length());
  if (!mount_point->catalog_mgr()->ListingStat(dir, &listing)) {
    *error = dir_path + ": listing failed";
    return false;
  }
  for (unsigned i = 0; i < listing.size(); ++i) {
    const string path = dir_path + "/" + listing.AtPtr(i)->name.ToString();
    catalog::DirectoryEntry dirent;
    if (!mount_point->catalog_mgr()->LookupPath(path, catalog::kLookupDefault,
                                                &dirent)) {
      *error = path + ": lookup failed";
      return false;
    }
    if (tree->files.size() + tree->symlinks.size()
        >= ConfigRepoTree::kMaxEntries) {
      *error = "too many entries";
      return false;
    }
    // Tree paths are relative to the repository root
    const string rel_path = path.substr(1);
    if (dirent.IsDirectory()) {
      if (!WalkTree(mount_point, path, total_size, tree, error))
        return false;
    } else if (dirent.IsLink()) {
      tree->symlinks[rel_path] = dirent.symlink().ToString();
    } else if (dirent.IsRegular()) {
      string content;
      if (!ReadObject(mount_point, path, dirent, &content, error))
        return false;
      *total_size += content.size();
      if (*total_size > ConfigRepoTree::kMaxTotalSize) {
        *error = "config repository tree too large";
        return false;
      }
      tree->files[rel_path] = content;
    }
  }
  return true;
}


static bool FetchInProcess(const string &config_repo,
                           OptionsManager *options_mgr, ConfigRepoTree *tree,
                           string *error) {
  FileSystem::FileSystemInfo fs_info;
  fs_info.name = "config-fetch";
  fs_info.type = FileSystem::kFsLibrary;
  fs_info.options_mgr = options_mgr;
  FileSystem *file_system = FileSystem::Create(fs_info);
  if (!file_system->IsValid()) {
    *error = "file system: " + file_system->boot_error();
    return false;
  }
  MountPoint *mount_point = MountPoint::Create(config_repo, file_system,
                                               options_mgr);
  if (!mount_point->IsValid()) {
    *error = "mount point: " + mount_point->boot_error();
    return false;
  }

  InterruptCue interrupt_cue;
  const ClientCtxGuard ctxg(geteuid(), getegid(), getpid(), &interrupt_cue);
  catalog::DirectoryEntry dirent;
  if (!mount_point->catalog_mgr()->LookupPath(string(kConfigSubtree),
                                              catalog::kLookupDefault, &dirent)
      || !dirent.IsDirectory()) {
    // An empty tree makes the parser treat the directory as missing
    LogCvmfs(kLogCvmfs, kLogDebug, "%s has no %s", config_repo.c_str(),
             kConfigSubtree);
  } else {
    uint64_t total_size = 0;
    if (!WalkTree(mount_point, kConfigSubtree, &total_size, tree, error))
      return false;
  }
  tree->fqrn = config_repo;
  tree->revision = mount_point->catalog_mgr()->GetRevision();
  tree->root_hash = mount_point->catalog_mgr()->GetRootHash().ToString();
  // Not torn down: the caller exits right away
  return true;
}


/**
 * Removes the CVMFS_* variables that a previous, environment-tainting parse
 * (e.g. before a reload) exported.  Otherwise the shell of the bootstrap
 * parse would see the values of the repository being mounted.
 */
static void ClearCvmfsEnvironment() {
  vector<string> names;
  for (char **env = environ; (env != NULL) && (*env != NULL); ++env) {
    const string entry(*env);
    if (HasPrefix(entry, "CVMFS_", false))
      names.push_back(entry.substr(0, entry.find('=')));
  }
  for (unsigned i = 0; i < names.size(); ++i)
    unsetenv(names[i].c_str());
}


/**
 * Downloads only the manifest to get the current root catalog hash.  It is
 * not verified: a forged "changed" only triggers a verified fetch, and a forged
 * "unchanged" is no worse than blocking the download.
 */
static bool ProbeRootHash(OptionsManager *options_mgr, string *root_hash,
                          string *error) {
  perf::Statistics statistics;
  download::DownloadManager download_mgr(
      1, perf::StatisticsTemplate("download", &statistics));
  string proxies;
  string fallback_proxies;
  options_mgr->GetValue("CVMFS_HTTP_PROXY", &proxies);
  options_mgr->GetValue("CVMFS_FALLBACK_PROXY", &fallback_proxies);
  download_mgr.SetProxyChain(proxies, fallback_proxies,
                             download::DownloadManager::kSetProxyBoth);
  string optarg;
  if (options_mgr->GetValue("CVMFS_SERVER_URL", &optarg))
    download_mgr.SetHostChain(optarg);
  unsigned timeout = 5;
  unsigned timeout_direct = 10;
  if (options_mgr->GetValue("CVMFS_TIMEOUT", &optarg))
    timeout = String2Uint64(optarg);
  if (options_mgr->GetValue("CVMFS_TIMEOUT_DIRECT", &optarg))
    timeout_direct = String2Uint64(optarg);
  download_mgr.SetTimeout(timeout, timeout_direct);

  const string url = "/.cvmfspublished";
  cvmfs::MemSink sink;
  download::JobInfo job(&url, false, true /* probe hosts */, NULL, &sink);
  const download::Failures retval = download_mgr.Fetch(&job);
  if (retval != download::kFailOk) {
    *error = string("manifest download failed: ")
             + download::Code2Ascii(retval);
    return false;
  }
  manifest::Manifest *manifest = manifest::Manifest::LoadMem(sink.data(),
                                                             sink.pos());
  if (manifest == NULL) {
    *error = "invalid manifest";
    return false;
  }
  *root_hash = manifest->catalog_hash().ToString();
  delete manifest;
  return true;
}


/**
 * Returns kFetchUnchanged without a full fetch if expected_root_hash is
 * still current.
 */
static FetchStatus BootstrapAndFetch(const string &config_repo,
                                     bool simple_parser,
                                     const string &workspace,
                                     unsigned timeout_s,
                                     const string &expected_root_hash,
                                     ConfigRepoTree *tree, string *error) {
  ClearCvmfsEnvironment();
  OptionsTemplateManager *templ = new DefaultOptionsTemplateManager(
      config_repo);
  OptionsManager *options_mgr;
  if (simple_parser)
    options_mgr = new SimpleOptionsParser(templ);
  else
    options_mgr = new BashOptionsManager(templ);
  options_mgr->set_taint_environment(false);
  options_mgr->ParseDefault(config_repo);
  PrepareFetchOptions(workspace, timeout_s, options_mgr);
  if (!expected_root_hash.empty()) {
    string root_hash;
    if (!ProbeRootHash(options_mgr, &root_hash, error))
      return kFetchFailed;
    if (root_hash == expected_root_hash)
      return kFetchUnchanged;
  }
  return FetchInProcess(config_repo, options_mgr, tree, error) ? kFetchOk
                                                               : kFetchFailed;
}


/**
 * Writes the fetch result to fd: 'O' + serialized tree, 'U' for unchanged, or
 * 'E' + error.
 */
static bool WriteResult(int fd, FetchStatus status, const ConfigRepoTree &tree,
                        const string &error) {
  string payload;
  switch (status) {
    case kFetchOk:
      payload = "O" + tree.Serialize();
      break;
    case kFetchUnchanged:
      payload = "U";
      break;
    case kFetchFailed:
      payload = "E" + error;
      break;
  }
  return SafeWrite(fd, payload.data(), payload.size());
}


int MainConfigFetch(int argc, char **argv) {
  // cvmfs2 __config_fetch__ <result fd> <config repo> <simple parser>
  //                         <workspace> <timeout> [<expected root hash>]
  if ((argc != 7) && (argc != 8))
    return 1;
  // Not stdout, which logging may write to
  const int fd_result = String2Int64(argv[2]);
  const string config_repo = argv[3];
  const bool simple_parser = (string(argv[4]) == "1");
  const string workspace = argv[5];
  const unsigned timeout_s = String2Uint64(argv[6]);
  const string expected_root_hash = (argc == 8) ? argv[7] : "";
  ConfigRepoTree tree;
  string error;
  const FetchStatus status = BootstrapAndFetch(
      config_repo, simple_parser, workspace, timeout_s, expected_root_hash,
      &tree, &error);
  return WriteResult(fd_result, status, tree, error) ? 0 : 1;
}


FetchStatus FetchTree(const string &config_repo, bool simple_parser,
                      const string &workspace, unsigned timeout_s,
                      const string &expected_root_hash, ConfigRepoTree *tree,
                      string *error) {
  // A fresh process instead of a plain fork: the caller can be multi-threaded
  // (e.g. the remount thread), so locks may be held in a forked copy
  int pipe_result[2];
  MakePipe(pipe_result);
  vector<string> command_line;
  command_line.push_back(platform_getexepath());
  command_line.push_back("__config_fetch__");
  command_line.push_back(StringifyInt(pipe_result[1]));
  command_line.push_back(config_repo);
  command_line.push_back(simple_parser ? "1" : "0");
  command_line.push_back(workspace);
  command_line.push_back(StringifyUint(timeout_s));
  if (!expected_root_hash.empty())
    command_line.push_back(expected_root_hash);

  set<int> preserve_fildes;
  preserve_fildes.insert(2);
  preserve_fildes.insert(pipe_result[1]);
  pid_t pid;
  if (!ManagedExec(command_line, preserve_fildes, map<int, int>(),
                   false /* drop_credentials */, false /* clear_env */,
                   false /* double_fork */, &pid)) {
    close(pipe_result[0]);
    close(pipe_result[1]);
    *error = "failed to start " + command_line[0];
    return kFetchFailed;
  }

  close(pipe_result[1]);
  // Collect the full output with a deadline before parsing it, so that a
  // stuck child cannot block the parser
  string output;
  const uint64_t deadline = platform_monotonic_time() + timeout_s;
  bool timed_out = false;
  while (true) {
    const uint64_t now = platform_monotonic_time();
    if (now >= deadline) {
      timed_out = true;
      break;
    }
    struct pollfd pfd;
    pfd.fd = pipe_result[0];
    pfd.events = POLLIN;
    pfd.revents = 0;
    const int retval = poll(&pfd, 1, (deadline - now) * 1000);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      break;
    }
    if (retval == 0)
      continue;
    char buf[4096];
    const ssize_t n = read(pipe_result[0], buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR)
        continue;
      break;
    }
    if (n == 0)
      break;
    output.append(buf, n);
    if (output.size() > ConfigRepoTree::kMaxTotalSize + 1024 * 1024) {
      *error = "child output too large";
      timed_out = true;
      break;
    }
  }
  close(pipe_result[0]);
  if (timed_out)
    kill(pid, SIGKILL);
  int statloc;
  while ((waitpid(pid, &statloc, 0) < 0) && (errno == EINTR)) {
  }
  if (timed_out) {
    if (error->empty())
      *error = "timeout after " + StringifyUint(timeout_s) + "s";
    return kFetchFailed;
  }
  if (!WIFEXITED(statloc) || (WEXITSTATUS(statloc) != 0) || output.empty()) {
    *error = "fetch process failed";
    return kFetchFailed;
  }
  if (output[0] == 'E') {
    *error = output.substr(1);
    return kFetchFailed;
  }
  if (output == "U")
    return kFetchUnchanged;

  if (!ConfigRepoTree::Deserialize(output.substr(1), tree)) {
    *error = "malformed fetch result";
    return kFetchFailed;
  }
  return kFetchOk;
}


static unsigned GetTimeout(const OptionsManager &options_mgr) {
  string optarg;
  if (options_mgr.GetValue("CVMFS_CONFIG_REPO_FETCH_TIMEOUT", &optarg)) {
    const uint64_t t = String2Uint64(optarg);
    if (t > 0)
      return t;
  }
  return kDefaultTimeout;
}


FetchResult FetchForRepository(const string &fqrn, bool simple_parser,
                               const string &expected_root_hash) {
  FetchResult result;
  OptionsManager *local_mgr;
  if (simple_parser)
    local_mgr = new SimpleOptionsParser();
  else
    local_mgr = new BashOptionsManager();
  local_mgr->set_taint_environment(false);
  // An empty fqrn restricts parsing to default.conf, default.d, default.local
  local_mgr->ParseDefault("");

  string optarg;
  if (local_mgr->GetValue("CVMFS_CONFIG_REPO_FETCH", &optarg))
    result.mode = ParseFetchMode(optarg);
  else
    result.mode = kFetchDirectFallback;
  result.required = local_mgr->GetValue("CVMFS_CONFIG_REPO_REQUIRED", &optarg)
                    && local_mgr->IsOn(optarg);
  local_mgr->GetValue("CVMFS_CONFIG_REPOSITORY", &result.config_repo);
  const unsigned timeout_s = GetTimeout(*local_mgr);
  string cache_base = kDefaultCacheBase;
  if (local_mgr->GetValue("CVMFS_CACHE_BASE", &optarg))
    cache_base = MakeCanonicalPath(optarg);
  delete local_mgr;

  if ((result.mode == kFetchMount) || result.config_repo.empty()
      || (result.config_repo == fqrn)) {
    result.mode = kFetchMount;
    return result;
  }

  ConfigRepoTree *tree = new ConfigRepoTree();
  const FetchStatus status = FetchTree(
      result.config_repo, simple_parser, cache_base + "/config-fetch/" + fqrn,
      timeout_s, expected_root_hash, tree, &result.error);
  result.unchanged = (status == kFetchUnchanged);
  if (status == kFetchOk)
    result.tree.reset(tree);
  else
    delete tree;
  return result;
}


}  // namespace config_repo
