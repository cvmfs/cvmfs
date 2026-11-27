/**
 * This file is part of the CernVM File System.
 *
 * Fetches the /etc/cvmfs tree of the config repository directly from the
 * server, so that option parsing does not depend on the mounted config
 * repository.
 */

#ifndef CVMFS_CONFIG_REPO_FETCH_H_
#define CVMFS_CONFIG_REPO_FETCH_H_

#include <memory>
#include <string>

#include "options.h"

namespace config_repo {

enum FetchMode {
  kFetchMount,
  kFetchDirect,
  kFetchDirectFallback,
};

FetchMode ParseFetchMode(const std::string &value);

/**
 * Removes "auto" (WPAD) proxy entries and makes DIRECT the last resort, so
 * that the config fetch has a small network surface.
 */
void RewriteProxies(std::string *proxies, std::string *fallback_proxies);

/**
 * The workspace of the shared quota manager that mounts with these options
 * use, or empty if they use another kind of cache.
 */
std::string SharedCacheWorkspace(const OptionsManager &options_mgr);

/**
 * Turns the options that mount the config repository into options for a
 * short-lived library mount with tight timeouts.  The fetch uses the shared
 * cache of the mounts if its cache manager runs, and otherwise an unmanaged
 * cache in the workspace.
 */
void PrepareFetchOptions(const std::string &workspace, unsigned timeout_s,
                         OptionsManager *options_mgr);

enum FetchStatus {
  kFetchOk,
  kFetchUnchanged,  ///< root hash equals the expected one, nothing fetched
  kFetchFailed,
};

/**
 * Fetches the etc/cvmfs subtree of config_repo in a helper process, which
 * parses the local configuration of config_repo for the server settings.  If
 * expected_root_hash is given, only the manifest is downloaded as long as it
 * still matches.  The helper is killed after timeout_s seconds.  *ttl is the
 * config repository's catalog TTL in seconds unless the fetch failed.
 */
FetchStatus FetchTree(const std::string &config_repo, bool simple_parser,
                      const std::string &workspace, unsigned timeout_s,
                      const std::string &expected_root_hash,
                      ConfigRepoTree *tree, unsigned *ttl,
                      std::string *error);

/**
 * Entry point of "cvmfs2 __config_fetch__", started by FetchTree().
 */
int MainConfigFetch(int argc, char **argv);

/**
 * The last successfully fetched tree, kept in the workspace so that mounts
 * within the TTL need no network and failed fetches can fall back to it.
 */
struct CachedTree {
  CachedTree() : fetch_time(0), ttl(0), generation(0) { }
  bool IsFresh(uint64_t now) const {
    return (fetch_time <= now) && (now < fetch_time + ttl);
  }

  uint64_t fetch_time;  ///< wall clock, last time the tree was confirmed
  unsigned ttl;
  /// Number of revisions fetched into the object cache since it was emptied
  unsigned generation;
  /// KeyDigest() of the keys that verified the tree
  std::string key_digest;
  ConfigRepoTree tree;
};

/**
 * Digest over the paths and contents of the public keys that verify the
 * config repository according to the options for the config repository.  A
 * cached tree is only trusted as long as these keys are unchanged.
 */
std::string KeyDigest(const OptionsManager &options_mgr);

bool LoadCachedTree(const std::string &path, CachedTree *cached);
bool StoreCachedTree(const std::string &path, const CachedTree &cached);

enum CachePolicy {
  kCacheUseFresh,   ///< no network while the cached tree is within its TTL
  kCacheRevalidate  ///< always check the config repository's revision
};

struct FetchResult {
  FetchResult() : mode(kFetchMount), required(false), stale(false) { }
  FetchMode mode;
  bool required;  ///< CVMFS_CONFIG_REPO_REQUIRED
  /// The fetch failed and tree is the last good copy from the workspace
  bool stale;
  std::string config_repo;
  std::string error;
  std::shared_ptr<const ConfigRepoTree> tree;  ///< NULL on failure or mount
};

/**
 * Reads the fetch mode from the local configuration and, unless it is
 * "mount", returns the config repository's tree for repository fqrn, from the
 * workspace of the config repository or from the server.  Concurrent callers
 * share one fetch.
 */
FetchResult FetchForRepository(const std::string &fqrn, bool simple_parser,
                               CachePolicy policy = kCacheUseFresh);

}  // namespace config_repo

#endif  // CVMFS_CONFIG_REPO_FETCH_H_
