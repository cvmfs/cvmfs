/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include "util/posix.h"

namespace publish {

Publisher::EFailures Publisher::CheckHealth(
  Publisher::ERepairMode mode, bool is_quiet)
{

  return kFailOk;
}

}  // namespace publish
