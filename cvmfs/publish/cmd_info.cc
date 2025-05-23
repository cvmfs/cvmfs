/**
 * This file is part of the CernVM File System.
 */


#include "cmd_info.h"

#include <ctime>
#include <string>

#include "options.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/logging.h"
#include "whitelist.h"

namespace publish {

int CmdInfo::Main(const Options &options) {
  SettingsBuilder builder;
  SettingsRepository settings = builder.CreateSettingsRepository(
      options.plain_args().empty() ? "" : options.plain_args()[0].value_str);

  if (options.Has("keychain")) {
    settings.GetKeychain()->SetKeychainDir(options.GetString("keychain"));
  }
  Repository repository(settings);

  LogCvmfs(kLogCvmfs, kLogStdout, "Repository name: %s",
           settings.fqrn().c_str());
  if (builder.IsManagedRepository()) {
    std::string creator_version;
    if (builder.options_mgr()->GetValue("CVMFS_CREATOR_VERSION",
                                        &creator_version)) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Created by CernVM-FS %s",
               creator_version.c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Configuration error: CVMFS_CREATOR_VERSION missing");
    }
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "Stratum1 replication allowed: %s",
           repository.IsMasterReplica() ? "yes" : "no");
  if (repository.whitelist()->IsExpired()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Whitelist is expired");
  } else {
    double delta_s = difftime(repository.whitelist()->expires(), time(NULL));
    int delta_d = static_cast<int>(delta_s / 86400);
    LogCvmfs(kLogCvmfs, kLogStdout, "Whitelist is valid for another %d days",
             delta_d);
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "\nClient configuration:\n"
           "Add %s to CVMFS_REPOSITORIES in /etc/cvmfs/default.local\n"
           "Create /etc/cvmfs/config.d/%s.conf and set\n"
           "  CVMFS_SERVER_URL=%s\n"
           "  CVMFS_PUBLIC_KEY=%s\n"
           "Copy %s to the client",
           settings.fqrn().c_str(), settings.fqrn().c_str(),
           settings.url().c_str(),
           settings.keychain().master_public_key_path().c_str(),
           settings.keychain().master_public_key_path().c_str());

  if (options.Has("meta-info")) {
    LogCvmfs(kLogCvmfs, kLogStdout, "\nMeta info:\n%s",
             repository.meta_info().c_str());
  }

  return 0;
}

}  // namespace publish
