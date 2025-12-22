/**
 * This file is part of the CernVM File System.
 */


#include <errno.h>

#include <cassert>

#ifdef __APPLE__
#include <unistd.h>
#endif

#include "util/capabilities.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

#ifdef __APPLE__

bool ClearPermittedCapabilities(int,
                                const cap_value_t *,
                                int,
                                const cap_value_t *) {
  return true;
}

namespace {

bool obtainCapability(const cap_value_t, const char *) {
  // there are no individual capabilities on OSX so switch back to root
  return (SwitchCredentials(0, getgid(), true));
}

bool checkCapabilityPermitted(const cap_value_t) {
  return (getuid() == 0);
}

} // namespace

#else

#include <sys/prctl.h>

// Clear all CAP_PERMITTED capabilities except those requested.
// This function requires being run with CAP_SETPCAP capability permitted.
// If the real uid & gid do not match the effective uid & gid, it also
// requires CAP_SETUID and CAP_SETGID capabilities to be permmitted and
// ends up switching the real uid & gid to match the incoming effective
// uid & gid.  Capabilities listed in inheritcaps will be made inheritable.

bool ClearPermittedCapabilities(int nreservecaps,
                                const cap_value_t *reservecaps,
                                int ninheritcaps,
                                const cap_value_t *inheritcaps) {
  int retval = 0;
  uid_t uid, gid;

  if (!SetpcapCapabilityPermitted()) {
    if (nreservecaps > 0) {
      LogCvmfs(kLogCvmfs, kLogDebug,
        "Capabilities cannot be reserved because setpcap is not permitted.");
      return false;
    }
    LogCvmfs(kLogCvmfs, kLogDebug,
      "Capabilities are already cleared because setpcap is not permitted.");
    return true;
  }
  
  uid = geteuid();
  gid = getegid();
  if ((uid != getuid()) || (gid != getgid())) {
    // Only do setuid & setgid when necessary because it is a process-wide
    // setting, not a per-thread setting.
    if (!ObtainSetuidgidCapabilities()) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogDebug,
          "Failed to obtain setuid/setgid capabilities"
          " while clearing capabilities (errno: %d)",
          errno);
      return false;
    }
    if (nreservecaps != 0) {
      // keep all capabilities when switching uid, and clear all but the
      // reserved ones below
      assert(platform_keepcaps(true));
    }
    retval = setgid(gid) || setuid(uid);
    if (nreservecaps != 0) {
      assert(platform_keepcaps(false));
    }
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogDebug,
          "Failed to set uid %d gid %d while clearing capabilities (errno: %d)",
          uid, gid, errno);
      return false;
    }
    if (nreservecaps == 0) {
      // all capabilities have been dropped
      return true;
    }
  }

  assert(ObtainSetpcapCapability());

  cap_t caps_proc = cap_get_proc();
  assert(caps_proc != NULL);

  for (int i = 0; i < nreservecaps; i++) {
    const cap_value_t cap = reservecaps[i];

#ifdef CAP_IS_SUPPORTED
    assert(CAP_IS_SUPPORTED(cap));
#endif

    cap_flag_value_t cap_state;
    retval = cap_get_flag(caps_proc, cap, CAP_PERMITTED, &cap_state);
    assert(retval == 0);
    if (cap_state != CAP_SET) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Warning: cap %d cannot be reserved. "
               "It's not in the process's permitted set.",
               cap);
    }
  }

  // Drop all EFFECTIVE, PERMITTED, and INHERITABLE capabilities other
  // than those requested PERMITTED & INHERITABLE capabilities.
  retval = cap_clear(caps_proc);
  assert(retval == 0);

  if (nreservecaps != 0) {
    retval = cap_set_flag(caps_proc, CAP_PERMITTED,
                          nreservecaps, reservecaps, CAP_SET);
    assert(retval == 0);
    if (ninheritcaps != 0) {
      retval = cap_set_flag(caps_proc, CAP_INHERITABLE,
                            ninheritcaps, inheritcaps, CAP_SET);
      assert(retval == 0);
    }
  }

  retval = cap_set_proc(caps_proc);
  const int saveerrno = errno;
  cap_free(caps_proc);

  if (retval != 0) {
    errno = saveerrno; // otherwise the linter doesn't see saveerrno as used
    LogCvmfs(kLogCvmfs, kLogDebug,
             "Cannot clear permitted capabilities for current process "
             "(errno: %d)",
             errno);
    return false;
  }

  if (ninheritcaps != 0) {
    for (int i = 0; i < ninheritcaps; i++) {
      retval = prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE,
                     inheritcaps[i], 0, 0);
      assert(retval == 0);
    }
  }

  return true;
}

namespace {

bool obtainCapability(const cap_value_t cap, const char *capname) {
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
    LogCvmfs(kLogCvmfs, kLogDebug,
             "Warning: %s cannot be obtained. "
             "It's not in the process's permitted set.",
             capname);
    cap_free(caps_proc);
    return false;
  }

  retval = cap_set_flag(caps_proc, CAP_EFFECTIVE, 1, &cap, CAP_SET);
  assert(retval == 0);

  retval = cap_set_proc(caps_proc);
  cap_free(caps_proc);

  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogDebug,
             "Cannot reset capabilities for current process "
             "(errno: %d)",
             errno);
    return false;
  }

  return true;
}

bool checkCapabilityPermitted(const cap_value_t cap) {
  cap_t caps_proc = cap_get_proc();
  assert(caps_proc != NULL);
  cap_flag_value_t cap_state;
  const int retval = cap_get_flag(caps_proc,
                                  cap,
                                  CAP_PERMITTED,
                                  &cap_state);
  assert(retval == 0);
  cap_free(caps_proc);
  return (cap_state == CAP_SET);
}

} // namespace

#endif // __APPLE__

bool ObtainDacReadSearchCapability() {
  return obtainCapability(CAP_DAC_READ_SEARCH, "CAP_DAC_READ_SEARCH");
}

bool ObtainSysAdminCapability() {
  return obtainCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
}

bool ObtainSysPtraceCapability() {
  return obtainCapability(CAP_SYS_PTRACE, "CAP_SYS_PTRACE");
}

bool ObtainSetuidgidCapabilities() {
  return (obtainCapability(CAP_SETUID, "CAP_SETUID") &&
    obtainCapability(CAP_SETGID, "CAP_SETGID"));
}

bool ObtainSetpcapCapability() {
  return (obtainCapability(CAP_SETPCAP, "CAP_SETPCAP"));
}

bool SetuidCapabilityPermitted() {
  return (checkCapabilityPermitted(CAP_SETUID));
}

bool SetpcapCapabilityPermitted() {
  return (checkCapabilityPermitted(CAP_SETPCAP));
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
