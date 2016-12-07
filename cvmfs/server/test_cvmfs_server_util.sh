#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Tests for "cvmfs_server_util.sh"

. ./cvmfs_server_util.sh

### check_overlayfs_version

mock_kernel_version="4.2.0"
cvmfs_sys_uname() {
    echo $mock_kernel_version
}

res=$(check_overlayfs_version; echo $?)
printf "Kernel version: %s; Calling check_overlayfs_version: %s\n" $mock_kernel_version $res

mock_kernel_version="4.1.1"

res=$(check_overlayfs_version; echo $?)
printf "Kernel version: %s; Calling check_overlayfs_version: %s\n" $mock_kernel_version $res

