/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_abort.h"

#include <cstdio>
#include <string>

#include "logging.h"
#include "publish/cmd_util.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "whitelist.h"

namespace publish {

int CmdAbort::Main(const Options &options) {
  SettingsBuilder builder;
  UniquePtr<SettingsPublisher> settings;
  try {
    settings = builder.CreateSettingsPublisher(
      options.plain_args().empty() ? "" : options.plain_args()[0].value_str,
      true /* needs_managed */);
  } catch (const EPublish &e) {
    if ((e.failure() == EPublish::kFailRepositoryNotFound) ||
        (e.failure() == EPublish::kFailRepositoryType))
    {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "CernVM-FS error: %s",
               e.msg().c_str());
      return 1;
    }
    throw;
  }

  if (settings->transaction().in_enter_session()) {
    throw EPublish(
      "aborting a transaction is unsupported within the ephemeral "
      "writable shell",
      EPublish::kFailInvocation);
  }

  if (!SwitchCredentials(settings->owner_uid(), settings->owner_gid(),
                         false /* temporarily */))
  {
    throw EPublish("No write permission to repository",
                   EPublish::kFailPermission);
  }

  if (HasPrefix(GetCurrentWorkingDirectory() + "/",
                settings->transaction().spool_area().union_mnt() + "/",
                false /* ignore_case */))
  {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Current working directory is in /cvmfs/%s.  Please release, "
             "e.g. by 'cd $HOME'.", settings->fqrn().c_str());
    return 1;
  }

  if (!options.Has("force")) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "You are about to DISCARD ALL CHANGES OF THE CURRENT TRANSACTION "
             "for %s!  Are you sure (y/N)? ", settings->fqrn().c_str());
    char answer[2];
    fgets(answer, 2, stdin);
    if ((answer[0] != 'Y') && (answer[0] != 'y'))
      return EINTR;
  }

  UniquePtr<Publisher> publisher;
  publisher = new Publisher(*settings);

  int rvi = CallServerHook("abort_before_hook", settings->fqrn());
  if (rvi != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "abort hook failed, not aborting");
    return rvi;
  }

  try {
    publisher->Abort();
  } catch (const EPublish &e) {
    if (e.failure() == EPublish::kFailTransactionState) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s", e.msg().c_str());
      return EINVAL;
    }
  }

  rvi = CallServerHook("abort_after_hook", settings->fqrn());
  if (rvi != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "post abort hook failed");
    return rvi;
  }

  return 0;
}

} // namespace publish
