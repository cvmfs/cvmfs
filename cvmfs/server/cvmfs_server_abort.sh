#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server abort command"
# Migrated to the new cvmfs_publish command

cvmfs_server_abort() {
  $(__publish_cmd dbg) abort $@
}
