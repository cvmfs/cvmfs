#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server help" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_help() {
  check_parameter_count 1 $#
  local command="$1"
  local comm_help_path="/usr/share/cvmfs-server/help/${command}.txt"

  if [ ! -f "$comm_help_path" ]; then
    usage "No manual entry for $command"
  fi

  cat "$comm_help_path"
}
