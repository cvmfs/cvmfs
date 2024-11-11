/**
 * This file is part of the CernVM File System.
 */


#include "swissknife_capabilities.h"

#include <errno.h>
#include <sys/capability.h>

#include <cassert>

#include "util/logging.h"

namespace swissknife {

bool ObtainDacReadSearchCapability() {
  cap_value_t cap = CAP_DAC_READ_SEARCH;
#ifdef CAP_IS_SUPPORTED
  assert(CAP_IS_SUPPORTED(cap));
#endif

  cap_t caps_proc = cap_get_proc();
  assert(caps_proc != NULL);

  cap_flag_value_t cap_state;
  int retval = cap_get_flag(caps_proc, cap, CAP_EFFECTIVE, &cap_state);
  assert(retval == 0);

  if (cap_state == CAP_SET) {
    cap_free(caps_proc);
    return true;
  }

  retval = cap_get_flag(caps_proc, cap, CAP_PERMITTED, &cap_state);
  assert(retval == 0);
  if (cap_state != CAP_SET) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Warning: CAP_DAC_READ_SEARCH cannot be obtained. "
             "It's not in the process's permitted set.");
    cap_free(caps_proc);
    return false;
  }

  retval = cap_set_flag(caps_proc, CAP_EFFECTIVE, 1, &cap, CAP_SET);
  assert(retval == 0);

  retval = cap_set_proc(caps_proc);
  cap_free(caps_proc);

  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot reset capabilities for current process "
             "(errno: %d)",
             errno);
    return false;
  }

  return true;
}

}  // namespace swissknife
