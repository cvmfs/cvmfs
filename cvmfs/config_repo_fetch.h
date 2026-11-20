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
 * Turns the options that mount the config repository into options for a
 * short-lived library mount with a RAM cache and tight timeouts.
 */
void PrepareFetchOptions(const std::string &workspace, unsigned timeout_s,
                         OptionsManager *options_mgr);

/**
 * Fetches the etc/cvmfs subtree of config_repo in a forked child process,
 * which parses the local configuration of config_repo for the server
 * settings.  The child is killed after timeout_s seconds.
 */
bool FetchTree(const std::string &config_repo, bool simple_parser,
               const std::string &workspace, unsigned timeout_s,
               ConfigRepoTree *tree, std::string *error);

struct FetchResult {
  FetchResult() : mode(kFetchMount), required(false) { }
  FetchMode mode;
  bool required;  ///< CVMFS_CONFIG_REPO_REQUIRED
  std::string config_repo;
  std::string error;
  std::shared_ptr<const ConfigRepoTree> tree;  ///< NULL on failure or mount
};

/**
 * Reads the fetch mode from the local configuration and, unless it is
 * "mount", fetches the config repository for repository fqrn.
 */
FetchResult FetchForRepository(const std::string &fqrn, bool simple_parser);

}  // namespace config_repo

#endif  // CVMFS_CONFIG_REPO_FETCH_H_
