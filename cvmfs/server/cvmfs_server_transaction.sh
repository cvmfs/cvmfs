#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server transaction command"
# Migrated to the new cvmfs_publish command

cvmfs_server_transaction() {
  if [ $# -gt 0 ]; then
    for lastarg in "$@"; do true; done
    repo_name_maybe_with_path=$lastarg
    repo_name=$(echo "$repo_name_maybe_with_path" | sed -e 's:/.*::')
    load_repo_config "$repo_name"
    export CVMFS_COMPRESSION_ALGORITHM
  fi
  $(__publish_cmd dbg) transaction $@
}
