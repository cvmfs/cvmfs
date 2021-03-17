/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_abort.h"

#include <string>

#include "logging.h"
#include "publish/except.h"
#include "publish/settings.h"
#include "util/pointer.h"

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
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "CernVM-FS error: "
             "aborting a transaction is unsupported within the ephemeral "
             "writable shell");
    return 1;
  }



  return 0;
}

} // namespace publish
