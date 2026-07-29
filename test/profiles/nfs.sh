# shellcheck shell=bash
#
# Profile: nfs
#
# Exercises the CernVM-FS NFS-export path, i.e. the leveldb-backed persistent
# inode maps (cvmfs/nfs_maps_leveldb.cc), and selects the NFS-specific test
# cases. 669-nfsexport additionally re-exports a repository through the
# in-kernel NFS server and reads it back over loopback NFS.
#
# NFS mode (CVMFS_NFS_SOURCE=yes) is enabled per read-only client mount by the
# individual test cases rather than globally via /etc/cvmfs/default.d: the same
# tests create and publish their fixture repository with cvmfs_server, whose own
# read-only spool mount must stay in the default (non-NFS) mode.
#
# The in-kernel NFS server (nfs-kernel-server / knfsd, rpcbind) must be
# installed and running before calling run.sh; see
# .github/workflows/ci_nfs.yml for the CI provisioning.

PROFILE_CLASS_NAME="NFSClientTests"
PROFILE_TESTSUITE="src/668-nfschunkinodes src/669-nfsexport"
# Select by the "nfs" suite label: 669-nfsexport is deliberately not in "quick"
# (it needs the in-kernel NFS server), so the default quick label would skip it.
PROFILE_LABELS="nfs"

profile_setup() {
  # The re-export test needs a working kernel NFS server.
  if ! command -v exportfs >/dev/null 2>&1; then
    echo "[profile:nfs] 'exportfs' not found - install nfs-kernel-server"
    return 1
  fi

  # nfsd (running as root) must be able to traverse the FUSE mount that a test
  # user created; this requires user_allow_other in the global fuse config.
  if ! grep -q '^[[:space:]]*user_allow_other' /etc/fuse.conf 2>/dev/null; then
    echo "[profile:nfs] enabling user_allow_other in /etc/fuse.conf"
    echo 'user_allow_other' | sudo tee -a /etc/fuse.conf >/dev/null || return 1
  fi

  echo "[profile:nfs] rpcinfo:"
  rpcinfo -p localhost 2>/dev/null || echo "[profile:nfs] (rpcbind not reachable - v3 mounts may fail)"
}
