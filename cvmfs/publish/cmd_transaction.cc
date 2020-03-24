/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_transaction.h"

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
  FileSystemInfo fs_info = GetFileSystemInfo("/cvmfs");
  if (fs_info.type == kFsTypeAutofs)
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

  int rvi = publisher.managed_node()->Check(Publisher::ManagedNode::kRepairSafe,
                                            false /* is_quiet */);
  if (rvi != 0) throw EPublish("cannot establish writable mountpoint");

  rvi = CallServerHook("transaction_before_hook", fqrn);
  if (rvi != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "transaction hook failed, not opening a transaction");
    return rvi;
  }

  if (!publisher.in_transaction() || options.HasNot("force")) {
    try {
      publisher.Transaction();
    } catch (const EPublish &e) {
      if (e.id() == EPublish::kIdTransactionLocked) {
        LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s", e.msg().c_str());
        return 1;
      }
      throw;
    }
  }

  publisher.managed_node()->Open();

  rvi = CallServerHook("transaction_after_hook", fqrn);
  if (rvi != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "post transaction hook failed");
    return rvi;
  }

  return 0;
}

}  // namespace publish
