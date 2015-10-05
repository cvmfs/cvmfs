/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTO_UMOUNT_H_
#define CVMFS_AUTO_UMOUNT_H_

#include <string>

namespace auto_umount {

void SetMountpoint(const std::string &mountpoint);
void UmountOnCrash();

}  // namespace auto_umount

#endif  // CVMFS_AUTO_UMOUNT_H_
