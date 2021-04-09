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
             "Current working directory is in %s.  Please release, "
             "e.g. by 'cd $HOME'.",
             settings->transaction().spool_area().union_mnt().c_str());
    return 1;
  }

  if (!options.Has("force")) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "You are about to DISCARD ALL CHANGES OF THE CURRENT TRANSACTION "
             "for %s!  Are you sure (y/N)? ", settings->fqrn().c_str());
    char answer[] = {0, 0, 0};
    char *rv_charp = fgets(answer, 3, stdin);
    if (rv_charp && (answer[0] != 'Y') && (answer[0] != 'y'))
      return EINTR;
  }

  std::vector<LsofEntry> lsof_entries =
    Lsof(settings->transaction().spool_area().union_mnt());
  if (!lsof_entries.empty()) {
    if (options.Has("force")) {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "WARNING: Open file descriptors on %s (possible race!)"
               "\nThe following lsof report might show the culprit:\n",
               settings->transaction().spool_area().union_mnt().c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "\nWARNING! There are open read-only file descriptors in %s\n"
               "  --> This is potentially harmful and might cause problems "
               "later on.\n"
               "      We can anyway perform the requested operation, but this "
               "will most likely\n"
               "      break other processes with open file descriptors on %s!\n"
               "\n"
               "      The following lsof report might show the processes with "
               "open file handles\n",
               settings->transaction().spool_area().union_mnt().c_str(),
               settings->transaction().spool_area().union_mnt().c_str());
    }

    for (unsigned i = 0; i < lsof_entries.size(); ++i) {
      std::string owner_name;
      GetUserNameOf(lsof_entries[i].owner, &owner_name);
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %d %s %s",
               lsof_entries[i].executable.c_str(),
               lsof_entries[i].pid,
               owner_name.c_str(),
               lsof_entries[i].path.c_str());
    }

    if (!options.Has("force")) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
               "\n      Do you want to proceed anyway? (y/N) ");
      char answer[] = {0, 0, 0};
      char *rv_charp = fgets(answer, 3, stdin);
      if (rv_charp && (answer[0] != 'Y') && (answer[0] != 'y'))
        return EINTR;
    }
  }

  UniquePtr<Publisher> publisher;
  publisher = new Publisher(*settings);

  LogCvmfs(kLogCvmfs, kLogSyslog, "(%s) aborting transaction",
           settings->fqrn().c_str());

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

}  // namespace publish
