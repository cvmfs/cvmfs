#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of deprecated cvmfs_server commands

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh
# - cvmfs_server_tag.sh


cvmfs_server_lstags() {
  cvmfs_server_tag -l "$@" # backward compatibility alias
  echo "NOTE: cvmfs_server lstags is deprecated! Use cvmfs_server tag instead" 1>&2
}

cvmfs_server_list_tags() {
  cvmfs_server_tag -l "$@" # backward compatibility alias
  echo "NOTE: cvmfs_server list-tags is deprecated! Use cvmfs_server tag instead" 1>&2
}
