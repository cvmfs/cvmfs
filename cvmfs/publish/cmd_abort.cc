/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_abort.h"

#include "publish/settings.h"

namespace publish {

int CmdAbort::Main(const Options &options) {
  SettingsBuilder builder;
  SettingsRepository settings = builder.CreateSettingsRepository(
    options.plain_args().empty() ? "" : options.plain_args()[0].value_str);

  return 0;
}

} // namespace publish
