#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server

# This file should implement all platform specific functions. The other components of the
# cvmfs_server script should interact with the underlying system only through the functions
# defined here. This allows simple(r) unit testing of the "cvmfs_server" script

cvmfs_sys_uname() {
    uname -r | grep -oe '^[0-9]\+\.[0-9]\+.[0-9]\+'
}

cvmfs_sys_is_regular_file() {
    [ -f $1 ]
    echo $?
}

