/**
 * This file is part of the CernVM File System.
 *
 * Mount table helpers for the NFS frontend.
 */

#ifndef CVMFS_NFS4_MOUNT_UTIL_H_
#define CVMFS_NFS4_MOUNT_UTIL_H_

#include <string>
#include <vector>

namespace nfs4 {

/** Runs args[0] with args, returns its exit code or a negative errno */
int RunCommand(const std::vector<std::string> &args);
/** Whether an NFS file system is mounted on mountpoint (without NFS I/O) */
bool IsMounted(const std::string &mountpoint);
/** Force-unmounts mountpoint */
bool UnmountNfs(const std::string &mountpoint);

}  // namespace nfs4

#endif  // CVMFS_NFS4_MOUNT_UTIL_H_
