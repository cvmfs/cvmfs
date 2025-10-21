/**
 * This file is part of the CernVM File System.
 */


#include "auto_umount.h"

#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <set>
#include <string>
#include <vector>

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

  // It is still mounted; now check to see if it is live.  That can happen
  // if the unmount happened due to an explicit external umount call
  // (e.g. the automounter) but then it quickly got mounted again.

  // stat() might be served from caches.  Opendir ensures fuse module is called.
  int expected_error;
#ifdef __APPLE__
  expected_error = ENXIO;
#else
  expected_error = ENOTCONN;
#endif
  DIR *dirp = opendir(mountpoint_->c_str());
  if (dirp) {
    closedir(dirp);
    LogCvmfs(kLogCvmfs, kLogSyslog,
             "%s cleanup handler: "
             "%s seems to be active, skipping unmount",
             cleanuptype, mountpoint_->c_str());
    return;
  }
  if (errno != expected_error) {
    LogCvmfs(kLogCvmfs, kLogSyslog,
             "%s cleanup handler: "
             "error when checking %s liveness (%d), skipping unmount",
             cleanuptype, mountpoint_->c_str(), errno);
    return;
  }

  // sudo umount -l *mountpoint_
  if (!SwitchCredentials(0, getegid(), true)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "%s cleanup handler: "
             "failed to re-gain root privileges",
             cleanuptype);
    return;
  }
  const bool lazy = true;
  bool retval = platform_umount(mountpoint_->c_str(), lazy);
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
