/**
 * This file is part of the CernVM File System.
 *
 * Translates cvmfs_init and cvmfs_repo_attach calls into their contemporary
 * counterparts.
 */


#include <cstdio>
#include <cstring>
#include <string>

#include "libcvmfs.h"
#include "libcvmfs_int.h"
#include "loader.h"
#include "options.h"
#include "util/string.h"

using namespace std;  // NOLINT

static int set_option(char const *name, char const *value, bool *var) {
  if (*value != '\0') {
    fprintf(stderr, "Option %s=%s contains a value when none was expected.\n",
            name, value);
    return -1;
  }
  *var = true;
  return 0;
}


static int set_option(char const *name, char const *value, unsigned *var) {
  unsigned v = 0;
  int end = 0;
  const int rc = sscanf(value, "%u%n", &v, &end);
  if (rc != 1 || value[end] != '\0') {
    fprintf(stderr, "Invalid unsigned integer value for %s=%s\n", name, value);
    return -1;
  }
  *var = v;
  return 0;
}


static int set_option(char const *name, char const *value, int *var) {
  int v = 0;
  int end = 0;
  const int rc = sscanf(value, "%d%n", &v, &end);
  if (rc != 1 || value[end] != '\0') {
    fprintf(stderr, "Invalid integer value for %s=%s\n", name, value);
    return -1;
  }
  *var = v;
  return 0;
}


static int set_option(char const *name, char const *value, string *var) {
  *var = value;
  return 0;
}


#define CVMFS_OPT(var)         \
  if (strcmp(name, #var) == 0) \
  return ::set_option(name, value, &var)


struct cvmfs_repo_options {
  int set_option(char const *name, char const *value) {
    CVMFS_OPT(allow_unsigned);
    CVMFS_OPT(blacklist);
    CVMFS_OPT(deep_mount);  // deprecated
    CVMFS_OPT(fallback_proxies);
    CVMFS_OPT(mountpoint);
    CVMFS_OPT(proxies);
    CVMFS_OPT(pubkey);
    CVMFS_OPT(repo_name);
    CVMFS_OPT(timeout);
    CVMFS_OPT(timeout_direct);
    CVMFS_OPT(tracefile);
    CVMFS_OPT(url);

    fprintf(stderr, "Unknown repo option: %s\n", name);
    return -1;
  }

  int verify_sanity() {
    if (mountpoint.empty() && !repo_name.empty()) {
      mountpoint = "/cvmfs/";
      mountpoint += repo_name;
    }
    while (mountpoint.length() > 0
           && mountpoint[mountpoint.length() - 1] == '/') {
      mountpoint.resize(mountpoint.length() - 1);
    }

    return LIBCVMFS_FAIL_OK;
  }

  cvmfs_repo_options()
      : timeout(2)
      , timeout_direct(2)
      , pubkey("/etc/cvmfs/keys/cern.ch.pub")
      , blacklist("")
      , allow_unsigned(false) { }

  unsigned timeout;
  unsigned timeout_direct;
  std::string url;
  std::string external_url;
  std::string proxies;
  std::string fallback_proxies;
  std::string tracefile;  // unused
  std::string pubkey;
  std::string deep_mount;
  std::string blacklist;
  std::string repo_name;
  std::string root_hash;
  std::string mountpoint;
  bool allow_unsigned;
};


struct cvmfs_global_options {
  int set_option(char const *name, char const *value) {
    CVMFS_OPT(alien_cache);
    CVMFS_OPT(alien_cachedir);
    CVMFS_OPT(cache_directory);
    CVMFS_OPT(cachedir);
    CVMFS_OPT(lock_directory);
    CVMFS_OPT(change_to_cache_directory);
    CVMFS_OPT(logfile);
    CVMFS_OPT(log_file);
    CVMFS_OPT(log_prefix);
    CVMFS_OPT(log_syslog_level);
    CVMFS_OPT(syslog_level);
    CVMFS_OPT(max_open_files);
    CVMFS_OPT(nofiles);
    CVMFS_OPT(quota_limit);
    CVMFS_OPT(quota_threshold);
    CVMFS_OPT(rebuild_cachedb);

    fprintf(stderr, "Unknown global option: %s\n", name);
    return LIBCVMFS_FAIL_BADOPT;
  }

  int verify_sanity() {
    // Alias handling
    if ((nofiles >= 0) && (max_open_files != 0) && (nofiles != max_open_files))
      return LIBCVMFS_FAIL_BADOPT;
    if (nofiles >= 0)
      max_open_files = nofiles;

    if ((syslog_level >= 0) && (log_syslog_level != 0)
        && (syslog_level != log_syslog_level)) {
      return LIBCVMFS_FAIL_BADOPT;
    }
    if (syslog_level >= 0)
      log_syslog_level = syslog_level;
    if (log_syslog_level < 0)
      log_syslog_level = 3;

    if ((logfile != "") && (log_file != "") && (log_file != logfile))
      return LIBCVMFS_FAIL_BADOPT;
    if (logfile != "")
      log_file = logfile;

    if ((cachedir != "") && (cache_directory != "")
        && (cache_directory != cachedir)) {
      return LIBCVMFS_FAIL_BADOPT;
    }
    if (cachedir != "")
      cache_directory = cachedir;

    return LIBCVMFS_FAIL_OK;
  }

  cvmfs_global_options()
      : change_to_cache_directory(false)
      , alien_cache(false)
      , syslog_level(-1)
      , log_syslog_level(-1)
      , nofiles(-1)
      , max_open_files(0)
      , quota_limit(0)
      , quota_threshold(0)
      , rebuild_cachedb(0) { }

  std::string cache_directory;
  std::string cachedir;  // Alias of cache_directory
  std::string alien_cachedir;
  std::string lock_directory;
  bool change_to_cache_directory;
  bool alien_cache;

  int syslog_level;
  int log_syslog_level;
  std::string log_prefix;
  std::string logfile;
  std::string log_file;

  int nofiles;
  int max_open_files;  // Alias of nofiles

  // Currently ignored
  unsigned quota_limit;
  unsigned quota_threshold;
  bool rebuild_cachedb;
};


/**
 * Structure to parse the file system options.
 */
template<class DerivedT>
struct cvmfs_options : public DerivedT {
  int set_option(char const *name, char const *value) {
    return DerivedT::set_option(name, value);
  }

  int parse_options(char const *options) {
    while (*options) {
      char const *next = options;
      string name;
      string value;

      // get the option name
      for (next = options; *next && *next != ',' && *next != '='; next++) {
        if (*next == '\\') {
          next++;
          if (*next == '\0')
            break;
        }
        name += *next;
      }

      if (*next == '=') {
        next++;
      }

      // get the option value
      for (; *next && *next != ','; next++) {
        if (*next == '\\') {
          next++;
          if (*next == '\0')
            break;
        }
        value += *next;
      }

      if (!name.empty() || !value.empty()) {
        const int result = set_option(name.c_str(), value.c_str());
        if (result != 0) {
          return result;
        }
      }

      if (*next == ',')
        next++;
      options = next;
    }

    return DerivedT::verify_sanity();
  }
};

typedef cvmfs_options<cvmfs_repo_options> repo_options;
typedef cvmfs_options<cvmfs_global_options> global_options;

/**
 * Display the usage message.
 */
static void usage() {
  struct cvmfs_repo_options const defaults;
  fprintf(
      stderr,
      "CernVM-FS version %s\n"
      "Copyright (c) 2009- CERN\n"
      "All rights reserved\n\n"
      "Please visit http://cernvm.cern.ch/project/info for license details "
      "and author list.\n\n"

      "libcvmfs options are expected in the form: option1,option2,option3,...\n"
      "Within an option, the characters , and \\ must be preceded by \\.\n\n"

      "There are two types of options (global and repository specifics)\n"
      "  cvmfs_init()        expects global options\n"
      "  cvmfs_attach_repo() expects repository specific options\n"

      "global options are:\n"
      " cache_directory/cachedir=DIR Where to store disk cache\n"
      " change_to_cache_directory  Performs a cd to the cache directory "
      "(performance tweak)\n"
      " alien_cache                Treat cache directory as alien cache\n"
      " alien_cachedir=DIR         Explicitly set an alien cache directory\n"
      " lock_directory=DIR         Directory for per instance lock files.\n"
      "                            Needs to be on a file system with POSIX "
      "locks.\n"
      "                            Should be different from alien cache "
      "directory."
      "                            \nDefaults to cache_directory.\n"
      " (log_)syslog_level=LEVEL   Sets the level used for syslog to "
      "DEBUG (1), INFO (2), or NOTICE (3).\n"
      "                            Default is NOTICE.\n"
      " log_prefix                 String to use as a log prefix in syslog\n"
      " log_file/logfile           Logs all messages to FILE instead of "
      "stderr and daemonizes.\n"
      "                            Makes only sense for the debug version\n"
      " nofiles/max_open_files     Set the maximum number of open files "
      "for CernVM-FS process (soft limit)\n\n"

      "repository specific options are:"
      " repo_name=REPO_NAME        Unique name of the mounted repository, "
      "e.g. atlas.cern.ch\n"
      " url=REPOSITORY_URL         The URL of the CernVM-FS server(s): "
      "'url1;url2;...'\n"
      " timeout=SECONDS            Timeout for network operations (default is "
      "%d)\n"
      " timeout_direct=SECONDS     Timeout for network operations without "
      "proxy "
      "(default is %d)\n"
      " proxies=HTTP_PROXIES       Set the HTTP proxy list, such as "
      "'proxy1|proxy2;DIRECT'\n"
      " fallback_proxies=PROXIES   Set the fallback proxy list, such as "
      "'proxy1;proxy2'\n"
      " tracefile=FILE             Trace FUSE opaerations into FILE\n"
      " pubkey=PEMFILE             Public RSA key that is used to verify the "
      "whitelist signature.\n"
      " allow_unsigned             Accept unsigned catalogs "
      "(allows man-in-the-middle attacks)\n"
      " deep_mount=PREFIX          Path prefix if a repository is mounted on a "
      "nested catalog,\n"
      "                            i.e. deep_mount=/software/15.0.1\n"
      " mountpoint=PATH            Path to root of repository, "
      "e.g. /cvmfs/atlas.cern.ch\n"
      " blacklist=FILE             Local blacklist for invalid certificates. "
      "Has precedence over the whitelist.\n",
      CVMFS_VERSION, defaults.timeout, defaults.timeout_direct);
}


/**
 * Translates from loader::Failure codes to legacy LIBCVMFS_FAIL_... constants.
 */
static int TranslateReturnValue(loader::Failures code) {
  const int unknown = -10;

  switch (code) {
    case loader::kFailOk:
      return LIBCVMFS_FAIL_OK;
    case loader::kFailUnknown:
      return unknown;  // missing constant
    case loader::kFailOptions:
      return LIBCVMFS_FAIL_BADOPT;
    case loader::kFailPermission:
      return LIBCVMFS_FAIL_NOFILES;
    case loader::kFailMount:
    case loader::kFailLoaderTalk:
    case loader::kFailFuseLoop:
    case loader::kFailLoadLibrary:
    case loader::kFailIncompatibleVersions:
      return unknown;
    case loader::kFailCacheDir:
      return LIBCVMFS_FAIL_MKCACHE;
    case loader::kFailPeers:
    case loader::kFailNfsMaps:
      return unknown;
    case loader::kFailQuota:
      return LIBCVMFS_FAIL_INITQUOTA;
    case loader::kFailMonitor:
    case loader::kFailTalk:
      return unknown;
    case loader::kFailSignature:
    case loader::kFailCatalog:
      return LIBCVMFS_FAIL_INITCACHE;
    case loader::kFailMaintenanceMode:
    case loader::kFailSaveState:
    case loader::kFailRestoreState:
    case loader::kFailOtherMount:
    case loader::kFailDoubleMount:
    case loader::kFailHistory:
    case loader::kFailRevisionBlacklisted:
      return unknown;
    case loader::kFailWpad:
      return LIBCVMFS_FAIL_INITCACHE;
    case loader::kFailLockWorkspace:
      return LIBCVMFS_FAIL_LOCKFILE;
    default:
      return unknown;
  }
}


SimpleOptionsParser *cvmfs_options_init_legacy(char const *legacy_options) {
  global_options global_opts;
  const int parse_result = global_opts.parse_options(legacy_options);
  if (parse_result != 0) {
    fprintf(stderr, "Invalid CVMFS global options: %s.\n", legacy_options);
    usage();
    return NULL;
  }

  SimpleOptionsParser *options_mgr = cvmfs_options_init();
  options_mgr->SetValue("CVMFS_CACHE_DIR", global_opts.cache_directory);
  if (!global_opts.lock_directory.empty()) {
    options_mgr->SetValue("CVMFS_WORKSPACE", global_opts.lock_directory);
  }
  if (global_opts.alien_cache) {
    options_mgr->SetValue("CVMFS_ALIEN_CACHE", global_opts.cache_directory);
  }
  if (!global_opts.alien_cachedir.empty()) {
    options_mgr->SetValue("CVMFS_ALIEN_CACHE", global_opts.alien_cachedir);
  }
  // Note: as of version 2.4 CVMFS_CWD_CACHE support was dropped due to
  // disproportional large complexity in the FileSystem class
  // if (global_opts.change_to_cache_directory) {
  //  options_mgr->SetValue("CVMFS_CWD_CACHE", "on");
  // }
  options_mgr->SetValue("CVMFS_SYSLOG_LEVEL",
                        StringifyInt(global_opts.log_syslog_level));
  if (!global_opts.log_prefix.empty()) {
    options_mgr->SetValue("CVMFS_SYSLOG_PREFIX", global_opts.log_prefix);
  }
  if (!global_opts.log_file.empty()) {
    options_mgr->SetValue("CVMFS_DEBUGLOG", global_opts.log_file);
  }
  if (global_opts.max_open_files > 0) {
    options_mgr->SetValue("CVMFS_NFILES",
                          StringifyInt(global_opts.max_open_files));
  }

  return options_mgr;
}


int cvmfs_init(char const *options) {
  SimpleOptionsParser *options_mgr = cvmfs_options_init_legacy(options);
  if (options_mgr == NULL) {
    fprintf(stderr, "Invalid CVMFS global options: %s.\n", options);
    usage();
    return LIBCVMFS_FAIL_BADOPT;
  }

  const loader::Failures result = LibGlobals::Initialize(options_mgr);
  LibGlobals::GetInstance()->set_options_mgr(options_mgr);
  if (result != loader::kFailOk)
    LibGlobals::CleanupInstance();
  return TranslateReturnValue(result);
}


SimpleOptionsParser *cvmfs_options_clone_legacy(SimpleOptionsParser *opts,
                                                const char *legacy_options) {
  // Parse options
  repo_options repo_opts;
  const int parse_result = repo_opts.parse_options(legacy_options);
  if ((parse_result != 0) || repo_opts.url.empty()) {
    return NULL;
  }

  SimpleOptionsParser *options_mgr = cvmfs_options_clone(opts);
  options_mgr->SwitchTemplateManager(
      new DefaultOptionsTemplateManager(repo_opts.repo_name));
  options_mgr->SetValue("CVMFS_FQRN", repo_opts.repo_name);
  options_mgr->SetValue("CVMFS_TIMEOUT", StringifyInt(repo_opts.timeout));
  options_mgr->SetValue("CVMFS_TIMEOUT_DIRECT",
                        StringifyInt(repo_opts.timeout_direct));
  options_mgr->SetValue("CVMFS_SERVER_URL", repo_opts.url);
  if (!repo_opts.external_url.empty()) {
    options_mgr->SetValue("CVMFS_EXTERNAL_URL", repo_opts.external_url);
  }
  if (repo_opts.proxies.empty()) {
    if (!options_mgr->IsDefined("CVMFS_HTTP_PROXY"))
      options_mgr->SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  } else {
    options_mgr->SetValue("CVMFS_HTTP_PROXY", repo_opts.proxies);
  }
  options_mgr->SetValue("CVMFS_FALLBACK_PROXY", repo_opts.fallback_proxies);
  options_mgr->SetValue("CVMFS_PUBLIC_KEY", repo_opts.pubkey);
  if (!repo_opts.blacklist.empty()) {
    options_mgr->SetValue("CVMFS_BLACKLIST", repo_opts.blacklist);
  }
  if (!repo_opts.root_hash.empty()) {
    options_mgr->SetValue("CVMFS_ROOT_HASH", repo_opts.root_hash);
  }

  return options_mgr;
}


LibContext *cvmfs_attach_repo(char const *options) {
  SimpleOptionsParser *options_mgr_base = cvmfs_options_init();
  SimpleOptionsParser *options_mgr = cvmfs_options_clone_legacy(
      options_mgr_base, options);
  cvmfs_options_fini(options_mgr_base);
  if (options_mgr == NULL) {
    fprintf(stderr, "Invalid CVMFS options: %s.\n", options);
    usage();
    return NULL;
  }

  string repo_name;
  const bool retval = options_mgr->GetValue("CVMFS_FQRN", &repo_name);
  assert(retval);
  LibContext *ctx = LibContext::Create(repo_name, options_mgr);
  assert(ctx != NULL);
  if (ctx->mount_point()->boot_status() != loader::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug, "failed attaching %s, %s (%d)",
             repo_name.c_str(), ctx->mount_point()->boot_error().c_str(),
             ctx->mount_point()->boot_status());
    delete ctx;
    return NULL;
  }
  ctx->set_options_mgr(options_mgr);
  return ctx;
}
