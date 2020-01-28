/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_transaction.h"

#include <ctime>
#include <string>
#include <vector>

#include "logging.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/posix.h"
#include "util/string.h"
#include "whitelist.h"

namespace publish {

int CmdTransaction::Main(const Options &options) {
  // Repository name and lease path are submitted as a single argument
  // for historical reasons
  std::string fqrn;
  std::string lease_path;
  if (!options.plain_args().empty()) {
    std::vector<std::string> tokens =
      SplitString(options.plain_args()[0].value_str, '/', 2);
    fqrn = tokens[0];
    if (tokens.size() == 2)
      lease_path = std::string("/") + tokens[1];
  }

  SettingsBuilder builder;
  SettingsPublisher settings =
    builder.CreateSettingsPublisher(fqrn, true /* needs_managed */);

  if (!SwitchCredentials(settings.owner_uid(), settings.owner_gid(),
                         false /* temporarily */))
  {
    throw EPublish("No write permission to repository");
  }
  if (GetFileSystemType("/cvmfs") == kFsTypeAutofs)
    throw EPublish("Autofs on /cvmfs has to be disabled");


  Publisher publisher(settings);
  if (publisher.whitelist()->IsExpired()) {
    throw EPublish("Repository whitelist for $name is expired");
  }
  double whitelist_valid_s =
    difftime(publisher.whitelist()->expires(), time(NULL));
  if (whitelist_valid_s < (12 * 60 * 60)) {
    LogCvmfs(kLogCvmfs, kLogStdout,
      "Warning: Repository whitelist stays valid for less than 12 hours!");
  }


  // Transaction before hook
  // publisher --> transaction
  // Transaction after hook

  return 0;
}

}  // namespace publish
