#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server fix-permissions" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_fix_permissions() {
  local num_overlayfs=$(find /etc/cvmfs/repositories.d -name server.conf 2>/dev/null \
    -exec grep "CVMFS_UNION_FS_TYPE=overlayfs" {} \; | wc -l)
  if [ $num_overlayfs -gt 0 ]; then
    ensure_swissknife_suid overlayfs
  else
    ensure_swissknife_suid other
  fi
}


