/**
 * This file is part of the CernVM File System.
 */


#include <errno.h>
#ifdef __APPLE__
#include <unistd.h>
#else
#include <sys/prctl.h>
#endif

#include <cassert>

#include "util/capabilities.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

#ifdef __APPLE__

bool ClearPermittedCapabilities(const std::vector<cap_value_t> &,
                                const std::vector<cap_value_t> &) {
  return true;
}

namespace {

uid_t old_uid;
gid_t old_gid;

bool ObtainCapability(const cap_value_t,
                      const char *,
                      const bool avoid_mutexes = false) {
  // there are no individual capabilities on OSX so switch to root.
  // Only save uid/gid before the first elevation; subsequent calls while
  // already root would overwrite them with 0 and the Drop would never
  // restore to the original non-root credentials.
  if (geteuid() != 0) {
    old_uid = geteuid();
    old_gid = getegid();
  }
  return (SwitchCredentials(0, getgid(), true, avoid_mutexes));
}

bool CheckCapabilityPermitted(const cap_value_t) {
  return (getuid() == 0);
}

bool DropCapability(const cap_value_t,
                    const char *,
                    const bool avoid_mutexes = false) {
  // there are no individual capabilities on OSX so temporarily back to user
  return (SwitchCredentials(old_uid, old_gid, true, avoid_mutexes));
}

} // namespace

#else

/**
 * Clear all CAP_PERMITTED capabilities except those reserved.
 * This function requires being run with CAP_SETPCAP capability permitted.
 * If the real uid & gid do not match the effective uid & gid, it also
 * requires CAP_SETUID and CAP_SETGID capabilities to be permitted and
 * ends up switching the real uid & gid to match the incoming effective
 * uid & gid.  Beware that switching the uid is not thread-safe; it is
 * process-wide and clears all capabilities from threads that do not
 * have keepcaps enabled.
 *
 * @param[in] reservecaps  vector of capabilities to reserve
 * @param[in] inheritcaps  vector of capabilities to make inheritable
 */
bool ClearPermittedCapabilities(const std::vector<cap_value_t> &reservecaps,
                                const std::vector<cap_value_t> &inheritcaps) {
  int retval = 0;
  uid_t uid, gid;
  const int nreservecaps = (int) reservecaps.size();
  const int ninheritcaps = (int) inheritcaps.size();

  {
    cap_t caps_proc = cap_get_proc();
    if (caps_proc == NULL) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Failed to get process capabilities (errno: %d)", errno);
      return false;
    }
    cap_flag_value_t cap_state;
    retval = cap_get_flag(caps_proc, CAP_SETPCAP, CAP_PERMITTED, &cap_state);
    cap_free(caps_proc);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Failed to inspect setpcap capability (errno: %d)", errno);
      return false;
    }
    if (cap_state != CAP_SET) {
      if (nreservecaps > 0) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "Capabilities cannot be reserved because setpcap is not "
                 "permitted.");
        return false;
      }
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Capabilities are already cleared because setpcap is not "
               "permitted.");
      return true;
    }
  }
  
  uid = geteuid();
  gid = getegid();
  if ((uid != getuid()) || (gid != getgid())) {
    // Only do setuid & setgid when necessary because it is a process-wide
    // setting, not a per-thread setting.
    if (!ObtainSetuidgidCapabilities()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
        "Failed to obtain setuid/setgid capabilities"
        " while clearing capabilities (errno: %d)",
        errno);
      return false;
    }
    if (nreservecaps != 0) {
      // keep all capabilities when switching uid, and clear all but the
      // reserved ones below
      if (!platform_keepcaps(true)) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
                 "Failed to retain capabilities while switching credentials "
                 "(errno: %d)", errno);
        return false;
      }
    }
    retval = setgid(gid) || setuid(uid);
    if (nreservecaps != 0) {
      if (!platform_keepcaps(false)) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
                 "Failed to stop retaining capabilities after switching "
                 "credentials (errno: %d)", errno);
        return false;
      }
    }
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
        "Failed to set uid %d gid %d while clearing capabilities (errno: %d)",
        uid, gid, errno);
      return false;
    }
    if (nreservecaps == 0) {
      // all capabilities have been dropped
      return true;
    }
  }

  if (!ObtainSetpcapCapability()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "Failed to obtain setpcap capability while clearing capabilities "
             "(errno: %d)", errno);
    return false;
  }

  cap_t caps_proc = cap_get_proc();
  if (caps_proc == NULL) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "Failed to get process capabilities (errno: %d)", errno);
    return false;
  }

  for (int i = 0; i < nreservecaps; i++) {
    const cap_value_t cap = reservecaps[i];

#ifdef CAP_IS_SUPPORTED
    if (!CAP_IS_SUPPORTED(cap)) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Capability 0x%x is not supported", cap);
      cap_free(caps_proc);
      return false;
    }
#endif

    cap_flag_value_t cap_state;
    retval = cap_get_flag(caps_proc, cap, CAP_PERMITTED, &cap_state);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Failed to inspect capability 0x%x (errno: %d)", cap, errno);
      cap_free(caps_proc);
      return false;
    }
    if (cap_state != CAP_SET) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Warning: cap 0x%x cannot be reserved. "
               "It's not in the process's permitted set.",
               cap);
    }
  }

  // Drop all EFFECTIVE, PERMITTED, and INHERITABLE capabilities other
  // than those requested PERMITTED & INHERITABLE capabilities.
  retval = cap_clear(caps_proc);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "Failed to clear process capabilities (errno: %d)", errno);
    cap_free(caps_proc);
    return false;
  }

  if (nreservecaps != 0) {
    retval = cap_set_flag(caps_proc, CAP_PERMITTED,
                          nreservecaps, reservecaps.data(), CAP_SET);
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Failed to reserve process capabilities (errno: %d)", errno);
      cap_free(caps_proc);
      return false;
    }
    if (ninheritcaps != 0) {
      retval = cap_set_flag(caps_proc, CAP_INHERITABLE,
                            ninheritcaps, inheritcaps.data(), CAP_SET);
      if (retval != 0) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
                 "Failed to make process capabilities inheritable (errno: %d)",
                 errno);
        cap_free(caps_proc);
        return false;
      }
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
      if (retval != 0) {
        LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
                 "Failed to raise ambient capability 0x%x (errno: %d)",
                 inheritcaps[i], errno);
        return false;
      }
    }
  }

  return true;
}

namespace {

bool ObtainCapability(const cap_value_t cap,
                      const char *capname,
                      const bool avoid_mutexes = false) {
#ifdef CAP_IS_SUPPORTED
  if (!CAP_IS_SUPPORTED(cap)) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Capability %s is not supported", capname);
    return false;
  }
#endif

  cap_t caps_proc = cap_get_proc();
  if (caps_proc == NULL) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot get process capabilities (errno: %d)", errno);
    return false;
  }

  cap_flag_value_t cap_state;
  int retval = cap_get_flag(caps_proc, cap, CAP_EFFECTIVE, &cap_state);
  if (retval != 0) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot inspect %s capability (errno: %d)", capname, errno);
    cap_free(caps_proc);
    return false;
  }

  if (cap_state == CAP_SET) {
    cap_free(caps_proc);
    return true;
  }

  retval = cap_get_flag(caps_proc, cap, CAP_PERMITTED, &cap_state);
  if (retval != 0) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot inspect %s capability (errno: %d)", capname, errno);
    cap_free(caps_proc);
    return false;
  }
  if (cap_state != CAP_SET) {
    if (!avoid_mutexes) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Warning: %s cannot be obtained. "
               "It's not in the process's permitted set.",
               capname);
    }
    cap_free(caps_proc);
    return false;
  }

  retval = cap_set_flag(caps_proc, CAP_EFFECTIVE, 1, &cap, CAP_SET);
  if (retval != 0) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot enable %s capability (errno: %d)", capname, errno);
    cap_free(caps_proc);
    return false;
  }

  retval = cap_set_proc(caps_proc);
  cap_free(caps_proc);

  if (retval != 0) {
    if (!avoid_mutexes) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot set %s capability for current process (errno: %d)",
               capname, errno);
    }
    return false;
  }

  return true;
}

bool DropCapability(const cap_value_t cap,
                    const char *capname,
                    const bool avoid_mutexes = false) {
#ifdef CAP_IS_SUPPORTED
  if (!CAP_IS_SUPPORTED(cap)) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Capability %s is not supported", capname);
    return false;
  }
#endif

  cap_t caps_proc = cap_get_proc();
  if (caps_proc == NULL) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot get process capabilities (errno: %d)", errno);
    return false;
  }

  cap_flag_value_t cap_state;
  int retval = cap_get_flag(caps_proc, cap, CAP_EFFECTIVE, &cap_state);
  if (retval != 0) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot inspect %s capability (errno: %d)", capname, errno);
    cap_free(caps_proc);
    return false;
  }

  if (cap_state == CAP_CLEAR) {
    cap_free(caps_proc);
    return true;
  }

  retval = cap_set_flag(caps_proc, CAP_EFFECTIVE, 1, &cap, CAP_CLEAR);
  if (retval != 0) {
    if (!avoid_mutexes)
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "Cannot disable %s capability (errno: %d)", capname, errno);
    cap_free(caps_proc);
    return false;
  }

  retval = cap_set_proc(caps_proc);
  cap_free(caps_proc);

  if (retval != 0) {
    if (!avoid_mutexes) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogDebug,
               "Cannot reset %s capability for current process (errno: %d)",
               capname, errno);
    }
    return false;
  }

  return true;
}

bool CheckCapabilityPermitted(const cap_value_t cap) {
  cap_t caps_proc = cap_get_proc();
  if (caps_proc == NULL) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "Cannot get process capabilities (errno: %d)", errno);
    return false;
  }
  cap_flag_value_t cap_state;
  const int retval = cap_get_flag(caps_proc,
                                  cap,
                                  CAP_PERMITTED,
                                  &cap_state);
  cap_free(caps_proc);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "Cannot inspect permitted capability 0x%x (errno: %d)", cap,
             errno);
    return false;
  }
  return (cap_state == CAP_SET);
}

} // namespace

#endif // __APPLE__

bool ObtainDacReadSearchCapability() {
  return ObtainCapability(CAP_DAC_READ_SEARCH, "CAP_DAC_READ_SEARCH");
}

bool DropDacReadSearchCapability() {
  return DropCapability(CAP_DAC_READ_SEARCH, "CAP_DAC_READ_SEARCH");
}

bool ObtainSysAdminCapability() {
  return ObtainCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
}

bool ObtainSysPtraceCapability() {
  return ObtainCapability(CAP_SYS_PTRACE, "CAP_SYS_PTRACE");
}

bool DropSysPtraceCapability() {
  return DropCapability(CAP_SYS_PTRACE, "CAP_SYS_PTRACE");
}

bool ObtainSetuidgidCapabilities(const bool avoid_mutexes) {
  return (ObtainCapability(CAP_SETUID, "CAP_SETUID", avoid_mutexes) &&
    ObtainCapability(CAP_SETGID, "CAP_SETGID", avoid_mutexes));
}

bool ObtainSetpcapCapability() {
  return (ObtainCapability(CAP_SETPCAP, "CAP_SETPCAP"));
}

bool SetuidCapabilityPermitted() {
  return (CheckCapabilityPermitted(CAP_SETUID));
}

bool SetpcapCapabilityPermitted() {
  return (CheckCapabilityPermitted(CAP_SETPCAP));
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
