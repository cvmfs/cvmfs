/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "repository.h"

#include <sys/capability.h>

#include "publish/except.h"

namespace publish {

void Repository::DropCapabilities() {
  cap_t caps = cap_get_proc();
  int retval = cap_clear(caps);
  cap_free(caps);
  if (retval != 0)
    throw EPublish("cannot clear process capabilities");
}

}  // namespace publish
