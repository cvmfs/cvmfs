/**
 * This file is part of the CernVM File System.
 *
 * Keeps an NFS mount answerable after the cvmfs2 process serving it died.
 */

#ifndef CVMFS_NFS4_GUARDIAN_H_
#define CVMFS_NFS4_GUARDIAN_H_

#include <string>

namespace nfs4 {

/**
 * Runs as root next to a cvmfs2 process that may drop its privileges.  On
 * control_fd (a socket), it serves mount ('M') and unmount ('U') requests,
 * answering one status byte (0: success).  When control_fd reaches EOF, i.e.
 * when cvmfs2 exited or crashed, and the file system is still mounted, it
 * answers every NFS request on listen_fd with an error and force-unmounts.
 *
 * A dead NFS server is far worse than a dead FUSE daemon: the kernel client
 * keeps retrying, and processes block on the mount (or, for binaries mapped
 * from it, fault forever), which on macOS can stall the whole system.  An
 * answering server turns these into I/O errors and lets the unmount succeed.
 *
 * Returns 0 once nothing is mounted anymore.
 */
int RunGuardian(int listen_fd, int control_fd, const std::string &mountpoint);

}  // namespace nfs4

#endif  // CVMFS_NFS4_GUARDIAN_H_
