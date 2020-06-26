/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_transaction.h"

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
      lease_path = MakeCanonicalPath(tokens[1]);
  }

  SettingsBuilder builder;
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
  if (options.Has("retry-timeout")) {
    settings->GetTransaction()->SetTimeout(options.GetInt("retry-timeout"));
  }

  if (!SwitchCredentials(settings->owner_uid(), settings->owner_gid(),
                         false /* temporarily */))
  {
    throw EPublish("No write permission to repository");
  }
  FileSystemInfo fs_info = GetFileSystemInfo("/cvmfs");
  if (fs_info.type == kFsTypeAutofs)
    throw EPublish("Autofs on /cvmfs has to be disabled");

  settings->GetTransaction()->SetLeasePath(lease_path);

  Publisher publisher(*settings);
  if (publisher.whitelist()->IsExpired()) {
    throw EPublish("Repository whitelist for $name is expired");
  }
  double whitelist_valid_s =
    difftime(publisher.whitelist()->expires(), time(NULL));
  if (whitelist_valid_s < (12 * 60 * 60)) {
    LogCvmfs(kLogCvmfs, kLogStdout,
      "Warning: Repository whitelist stays valid for less than 12 hours!");
  }

  int rvi = publisher.managed_node()->Check(false /* is_quiet */);
  if (rvi != 0) throw EPublish("cannot establish writable mountpoint");

  rvi = CallServerHook("transaction_before_hook", fqrn);
  if (rvi != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "transaction hook failed, not opening a transaction");
    return rvi;
  }

  try {
    publisher.Transaction();
  } catch (const EPublish &e) {
    const char *msg_prefix = "CernVM-FS transaction error: ";
    if (e.failure() == EPublish::kFailTransactionLocked) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s%s",
               msg_prefix, e.msg().c_str());
      return EEXIST;
    } else if (e.failure() == EPublish::kFailLeaseBusy) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s%s",
               msg_prefix, e.msg().c_str());
      return EBUSY;
    } else if (e.failure() == EPublish::kFailLeaseNoEntry) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s%s",
               msg_prefix, e.msg().c_str());
      return ENOENT;
    } else if (e.failure() == EPublish::kFailLeaseNoDir) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%%s",
               msg_prefix, e.msg().c_str());
      return ENOTDIR;
    }
    throw;
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
