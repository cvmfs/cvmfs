/**
 * This file is part of the CernVM File System.
 */


#include "publish/except.h"
#include "repository.h"
#include "util/capabilities.h"
#include "util/platform.h"
#include "util/posix.h"

namespace publish {

void Env::DropCapabilities() {
  // Because the process has file capabilities, its dumpable state is set to
  // false, which in turn makes the /proc/self/... files owned by root.  We
  // need to reset this to have them owned by the effective UID in order to
  // set, e.g., uid_map/gid_map of user namespaces.
  if (!platform_set_dumpable())
    throw EPublish("cannot set dumpable state");

  const std::vector<cap_value_t> nocaps;
  if (!ClearPermittedCapabilities(nocaps, nocaps))
    throw EPublish("cannot clear process capabilities");
}


std::string Env::GetEnterSessionDir() {
  if (SymlinkExists("/.cvmfsenter"))
    return ResolvePath("/.cvmfsenter");
  return "";
}

}  // namespace publish
