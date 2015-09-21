/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "auto_umount.h"

#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <set>
#include <string>
#include <vector>

#include "logging.h"
#include "platform.h"
#include "util.h"

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


void UmountOnCrash() {
  if (!mountpoint_) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "crash cleanup handler: no mountpoint");
    return;
  }

  std::vector<std::string> all_mountpoints = platform_mountlist();
  if (all_mountpoints.empty()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "crash cleanup handler: "
             "failed to read mount point list");
    return;
  }

  // Mitigate auto-mount - crash - umount - auto-mount loops
  SafeSleepMs(2000);

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
    LogCvmfs(kLogCvmfs, kLogSyslog, "crash cleanup handler: %s not mounted",
             mountpoint_->c_str());
    return;
  }

  // stat() might be served from caches.  Opendir ensures fuse module is called.
  int expected_error;
#ifdef __APPLE__
  expected_error = ENXIO;
#else
  expected_error = ENOTCONN;
#endif
  DIR *dirp = opendir(mountpoint_->c_str());
  if (dirp || (errno != expected_error)) {
    if (dirp) closedir(dirp);
    LogCvmfs(kLogCvmfs, kLogSyslog, "crash cleanup handler: "
             "%s seems not to be stalled (%d)", mountpoint_->c_str(), errno);
    return;
  }

  // sudo umount -l *mountpoint_
  if (!SwitchCredentials(0, getegid(), true)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "crash cleanup handler: "
             "failed to re-gain root privileges");
    return;
  }
  const bool lazy = true;
  bool retval = platform_umount(mountpoint_->c_str(), lazy);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "crash cleanup handler: "
             "failed to unmount %s", mountpoint_->c_str());
    return;
  }

  LogCvmfs(kLogCvmfs, kLogSyslog, "crash cleanup handler unmounted stalled %s",
           mountpoint_->c_str());
}

}  // namespace auto_umount
