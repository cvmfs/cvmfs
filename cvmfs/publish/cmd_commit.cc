/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_commit.h"

#include <errno.h>

#include <ctime>
#include <string>
#include <vector>

#include "logging.h"
#include "publish/cmd_util.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/posix.h"
#include "util/string.h"
#include "whitelist.h"

namespace publish {

int CmdCommit::Main(const Options &options) {
  // Repository name and lease path are submitted as a single argument
  // for historical reasons
  std::string fqrn;
  std::string lease_path;

  if (!options.plain_args().empty()) {
    std::vector<std::string> tokens =
      SplitString(options.plain_args()[0].value_str, '/', 2);
    fqrn = tokens[0];
    if (tokens.size() == 2)
      lease_path = MakeCanonicalPath(tokens[1]);
  }

  // Create SettingsPublisher object
  SettingsBuilder builder;

  if (options.Has("repo-config")) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "External configuration for the repository");
    repo_config_ = options.GetString("repo-config");
    builder.config_path_ = repo_config_;
  }

  UniquePtr<SettingsPublisher> settings;
  try {
    LogCvmfs(kLogCvmfs, kLogStdout, "cmd_commit.cc");
    settings = builder.CreateSettingsPublisher(fqrn, true /* needs_managed */);
  } catch (const EPublish &e) {
    if (e.failure() == EPublish::kFailRepositoryNotFound) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "CernVM-FS error: %s",
               e.msg().c_str());
      return 1;
    }
    throw;
  }

  // Do we need read-write permission? yes
  if (!SwitchCredentials(settings->owner_uid(), settings->owner_gid(),
                         false /* temporarily */))
  {
    throw EPublish("No write permission to repository");
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "--- SWITCH CREDENTIALS DONE ---");

  // Do we need to check for autofs? yes
  FileSystemInfo fs_info = GetFileSystemInfo("/cvmfs");
  if (fs_info.type == kFsTypeAutofs)
    throw EPublish("Autofs on /cvmfs has to be disabled");

  // Do we need to get a lease? yes, because of the gateway
  settings->GetTransaction()->SetLeasePath(lease_path);

  // Create Publisher object
  UniquePtr<Publisher> publisher;
  try {
    publisher = new Publisher(*settings);
    if (publisher->whitelist()->IsExpired()) {
      throw EPublish("Repository whitelist for $name is expired",
                     EPublish::kFailWhitelistExpired);
    }
  } catch (const EPublish &e) {    if (e.failure() == EPublish::kFailLayoutRevision ||
        e.failure() == EPublish::kFailWhitelistExpired)
    {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s", e.msg().c_str());
      return EINVAL;
    }
  }

  double whitelist_valid_s =
    difftime(publisher->whitelist()->expires(), time(NULL));
  if (whitelist_valid_s < (12 * 60 * 60)) {
    LogCvmfs(kLogCvmfs, kLogStdout,
      "Warning: Repository whitelist stays valid for less than 12 hours!");
  }

  publisher->session()->SetKeepAlive(true);
  publisher->Sync();

  return 0;
}

}  // namespace publish
