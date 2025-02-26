/**
 * This file is part of the CernVM File System.
 */


#include "repository.h"

#include <sys/capability.h>
#include <sys/prctl.h>
#include <unistd.h>

#include "publish/except.h"
#include "util/posix.h"

namespace publish {

void Env::DropCapabilities() {
  int retval;

  // Because the process has file capabilities, its dumpable state is set to
  // false, which in turn makes the /proc/self/... files owned by root.  We
  // need to reset this to have them owned by the effective UID in order to
  // set, e.g., uid_map/gid_map of user namespaces.
  retval = prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);
  if (retval != 0)
    throw EPublish("cannot clear dumpable state");

  cap_t caps = cap_get_proc();
  retval = cap_clear(caps);
  cap_free(caps);
  if (retval != 0)
    throw EPublish("cannot clear process capabilities");
}


std::string Env::GetEnterSessionDir() {
  if (SymlinkExists("/.cvmfsenter"))
    return ResolvePath("/.cvmfsenter");
  return "";
}

}  // namespace publish
