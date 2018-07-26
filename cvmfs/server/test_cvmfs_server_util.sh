#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Tests for "cvmfs_server_util.sh"

. ./cvmfs_server_util.sh

print_check() {
    echo "Got: $1 Expected: $2"
}

### Testing check_overlayfs_version

check_overlayfs() {
    return 0
}

cvmfs_sys_uname() {
    echo $mock_kernel_version
}

# check_overlayfs_version will call cvmfs_sys_is_redhat
cvmfs_sys_is_redhat() {
    return $mock_is_redhat
}


mock_is_redhat=0 # true

mock_kernel_version="4.2.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0

mock_kernel_version="4.2.0-100"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0

mock_kernel_version="3.10.0-493"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0

mock_kernel_version="3.10.0-999"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0

mock_kernel_version="3.10.0-492"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 1

mock_kernel_version="3.10.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 1

mock_is_redhat=1 # False

mock_kernel_version="3.10.0-493"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 1

mock_kernel_version="3.10.0-492"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 1

mock_kernel_version="3.10.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 1

mock_kernel_version="4.2.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0

mock_kernel_version="4.2.0-100"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >/dev/null; echo $?) 0


################################################################################


mock_repo_name="test"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 0

mock_repo_name="test_1-dash.cern.ch"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 0

mock_repo_name=""
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 1

mock_repo_name="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQERSTUVWXYZ0123456"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 0

mock_repo_name="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQERSTUVWXYZ01234567"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 1

mock_repo_name="test_@1-dash.cern.ch"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 1

mock_repo_name="_test.cern.ch"
echo "Checking validity of repository name '$mock_repo_name'"
print_check $(is_valid_repo_name $mock_repo_name; echo $?) 1
