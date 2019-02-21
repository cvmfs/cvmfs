/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_mkfs.h"

#include "publish/settings.h"

namespace publish {

int CmdMkfs::Main(const Options &options) {
  std::string fqrn = options.plain_args()[0].value_str;
  // Sanitize fqrn
  // Check for root
  // determine union fs
  // set hash algorithm
  // set compression algorithm
  // set storage locator

  SettingsPublisher settings(fqrn);

  return 0;
}

}  // namespace publish
