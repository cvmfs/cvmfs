/**
 * This file is part of the CernVM File System.
 *
 * Mount table helpers for the NFS frontend.
 */

#ifndef CVMFS_NFS4_MOUNT_UTIL_H_
#define CVMFS_NFS4_MOUNT_UTIL_H_

#include <stdint.h>
#include <sys/types.h>

#include <string>
#include <vector>

namespace nfs4 {

/**
 * Regains root for its lifetime if the process dropped privileges only
 * temporarily (effective uid), as cvmfs2 does when started by root with
 * CVMFS_USER.  Mounting and unmounting need root.  Note that the effective
 * uid is per process, not per thread.
 */
class ScopedRoot {
 public:
  ScopedRoot();
  ~ScopedRoot();

 private:
  bool switched_;
  uid_t uid_;
  gid_t gid_;
};

/** Runs args[0] with args, returns its exit code or a negative errno */
int RunCommand(const std::vector<std::string> &args);
/** Default NFS mount options for a server on 127.0.0.1:port */
std::string NfsMountOptions(uint16_t port);
/**
 * Mounts the server on 127.0.0.1:port; CVMFS_NFS_MOUNT_OPTIONS from the
 * environment is appended to the options.
 */
bool MountNfs(const std::string &mountpoint, uint16_t port);
/**
 * Identifies the NFS mount on mountpoint served on port, 0 if there is none.
 * A later mount on the same path (e.g. after an external unmount) gets a
 * different id.  On macOS this is the kernel's fsid, as the
 * mount table does not show the port; elsewhere it is the port.  Reads the
 * mount table only, no NFS I/O.
 */
uint64_t FindMount(const std::string &mountpoint, uint16_t port);
/** Whether the mount identified by FindMount() is still in place */
bool IsMounted(const std::string &mountpoint, uint64_t mount_id);
/** Force-unmounts mountpoint */
bool UnmountNfs(const std::string &mountpoint);

}  // namespace nfs4

#endif  // CVMFS_NFS4_MOUNT_UTIL_H_
