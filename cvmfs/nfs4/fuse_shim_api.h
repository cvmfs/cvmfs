/**
 * This file is part of the CernVM File System.
 *
 * Interface of libcvmfs_nfsfuse beyond the libfuse API, used by the loader.
 */

#ifndef CVMFS_NFS4_FUSE_SHIM_API_H_
#define CVMFS_NFS4_FUSE_SHIM_API_H_

extern "C" {
/**
 * The session loop mounts the file system and then writes one byte to fd:
 * 0 on success, 1 on failure, closes fd, and redirects stdin/stdout/stderr
 * to /dev/null.  Lets a daemonizing loader exit only once the mount is in
 * place.
 */
void cvmfs_nfsfuse_set_ready_fd(int fd);
}

#endif  // CVMFS_NFS4_FUSE_SHIM_API_H_
