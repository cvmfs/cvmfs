/**
 * This file is part of the CernVM File System.
 */


#include "auto_umount.h"

#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "util/capabilities.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace auto_umount {

string *mountpoint_ = NULL;

void SetMountpoint(const string &mountpoint) {
  if (mountpoint == "") {
    delete mountpoint_;
    mountpoint_ = NULL;
  } else {
    mountpoint_ = new string(mountpoint);
  }
}


void UmountOnExit(const bool crashed) {
  const char *cleanuptype = "exit";
  if (crashed)
    cleanuptype = "crash";

  if (!mountpoint_) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "%s cleanup handler: no mountpoint",
             cleanuptype);
    return;
  }

  std::vector<std::string> all_mountpoints = platform_mountlist();
  if (all_mountpoints.empty()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "%s cleanup handler: "
             "failed to read mount point list",
             cleanuptype);
    return;
  }

  if (crashed) {
    // Mitigate auto-mount - crash - umount - auto-mount loops
    SafeSleepMs(2000);
  }

  // Check if *mountpoint_ is still mounted
  // (we don't want to trigger a mount by immediately doing stat *mountpoint_)
  bool still_mounted = false;
  for (unsigned i = 0; i < all_mountpoints.size(); ++i) {
    if (*mountpoint_ == all_mountpoints[i]) {
      still_mounted = true;
      break;
    }
  }
  if (!still_mounted) {
    int logtype = kLogDebug;
    if (crashed)
      logtype = kLogSyslog;
    LogCvmfs(kLogCvmfs, logtype,
             "%s cleanup handler: %s not mounted",
             cleanuptype, mountpoint_->c_str());
    return;
  }

  if (crashed) {
    // stat() might be served from caches.  Opendir ensures fuse module is called.
    int expected_error;
#ifdef __APPLE__
    expected_error = ENXIO;
#else
    expected_error = ENOTCONN;
#endif
    DIR *dirp = opendir(mountpoint_->c_str());
    if (dirp || (errno != expected_error)) {
      if (dirp)
        closedir(dirp);
      LogCvmfs(kLogCvmfs, kLogSyslog, "crash cleanup handler: "
               "%s seems not to be stalled (%d)",
               mountpoint_->c_str(), errno);
      return;
    }
  }

  // sudo umount -l *mountpoint_
  if (!ObtainSysAdminCapability()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "%s cleanup handler: "
             "failed to re-gain sys_admin capability",
             cleanuptype);
    return;
  }
  const bool lazy = true;
  bool const retval = platform_umount(mountpoint_->c_str(), lazy);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "%s cleanup handler: "
             "failed to unmount %s",
             cleanuptype, mountpoint_->c_str());
    return;
  }

  if (crashed) {
    LogCvmfs(kLogCvmfs, kLogSyslog,
           "crash cleanup handler unmounted stalled %s",
           mountpoint_->c_str());
  } else {
    LogCvmfs(kLogCvmfs, kLogSyslog,
           "exit cleanup handler unmounted %s",
           mountpoint_->c_str());
  }
}

}  // namespace auto_umount
