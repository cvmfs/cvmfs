/**
 * This file is part of the CernVM File System.
 */


#include "cmd_commit.h"

#include <errno.h>

#include <ctime>
#include <string>
#include <vector>

#include "publish/cmd_util.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"
#include "whitelist.h"

namespace publish {

int CmdCommit::Main(const Options &options) {
  // Repository name and lease path are submitted as a single argument
  // for historical reasons
  std::string fqrn;

  if (!options.plain_args().empty()) {
    std::vector<std::string> tokens =
      SplitStringBounded(2, options.plain_args()[0].value_str, '/');
    fqrn = tokens[0];
  }

  SettingsBuilder builder;
  std::string session_dir = Env::GetEnterSessionDir();
  builder.SetConfigPath(session_dir);

  UniquePtr<SettingsPublisher> settings;
  try {
    settings = builder.CreateSettingsPublisher(fqrn, true /* needs_managed */);
  } catch (const EPublish &e) {
    if (e.failure() == EPublish::kFailRepositoryNotFound) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "CernVM-FS error: %s",
               e.msg().c_str());
      return 1;
    }
    throw;
  }

  if (!SwitchCredentials(settings->owner_uid(), settings->owner_gid(),
                         false /* temporarily */))
  {
    throw EPublish("No write permission to repository");
  }

  FileSystemInfo fs_info = GetFileSystemInfo("/cvmfs");
  if (fs_info.type == kFsTypeAutofs)
    throw EPublish("Autofs on /cvmfs has to be disabled");

  UniquePtr<Publisher> publisher;
  try {
    publisher = new Publisher(*settings);
    if (publisher->whitelist()->IsExpired()) {
      throw EPublish("Repository whitelist for $name is expired",
                     EPublish::kFailWhitelistExpired);
    }
  } catch (const EPublish &e) {
    if (e.failure() == EPublish::kFailLayoutRevision ||
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

  publisher->Sync();
  SafeWriteToFile("commit", session_dir + "/shellaction.marker", 0600);
  LogCvmfs(kLogCvmfs, kLogStdout, "Changes saved!");
  publisher->ExitShell();

  return 0;
}

}  // namespace publish
