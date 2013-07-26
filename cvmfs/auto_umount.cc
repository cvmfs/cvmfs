/**
 * This file is part of the CernVM File System.
 */

#include "config.h"
#include "auto_umount.h"

#include "logging.h"

using namespace std;

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
}

}  // namespace auto_umount
