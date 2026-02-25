#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Tests for "cvmfs_server_util.sh"

. ./cvmfs_server_util.sh

FAILS=0

print_check() {
    echo "Got: $1 Expected: $2"
    if [ "$1" != "$2" ]; then
        FAILS=$(($FAILS + 1))
    fi
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
print_check $(check_overlayfs_version >&2; echo $?) 0

mock_kernel_version="4.2.0-100"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 0

mock_kernel_version="3.10.0-493"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 0

mock_kernel_version="3.10.0-999"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 0

mock_kernel_version="3.10.0-492"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 1

mock_kernel_version="3.10.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 1

mock_is_redhat=1 # False

mock_kernel_version="3.10.0-493"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 1

mock_kernel_version="3.10.0-492"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 1

mock_kernel_version="3.10.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 1

mock_kernel_version="4.2.0"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 0

mock_kernel_version="4.2.0-100"
printf "Kernel version: %s; RedHat: %s\n" $mock_kernel_version $mock_is_redhat
print_check $(check_overlayfs_version >&2; echo $?) 0


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

#--------------------------------

echo "Starting two background processes b & c to test locks"

die() { echo "$@" >&2; exit 1; }

LOCKDIR="$(mktemp -d)"

RET=0

# PS4 gives extra header in bash when set -x is enabled, showing active proc
PS4='a+ '

# Lock names starting with "lock" are the real tests; lock names starting
# with "shake" are just for handshakes between processes although they also
# help to test the functions.

# Can't use simple "( ... ) &" form because that does not re-initialize $$,
# which is needed by the lock functions.  Variables we don't want expanded
# by the parent shell need to be escaped with a backslash.
$SHELL <<!EOF! &
. ./cvmfs_server_util.sh
die() { echo "\$@" >&2; exit 1; }
PS4='b+ '
RET=0
acquire_lock $LOCKDIR/lock1 1
echo "b has lock1"
if ! acquire_lock $LOCKDIR/lock2; then
  echo "proc1 unexpectedly couldn't acquire lock2" >&2
  RET=\$((\$RET + 1))
fi
echo "b has lock2"
acquire_lock $LOCKDIR/shake1 1
echo "b sent shake1"
while ! check_lock $LOCKDIR/shake3; do
  echo "b waiting for shake3"
  sleep 1
done
release_lock $LOCKDIR/shake1
release_lock $LOCKDIR/lock2
echo "b released lock2"
while ! check_lock $LOCKDIR/shake5; do
  echo "b waiting for shake5"
  sleep 1
done
release_lock $LOCKDIR/lock1
echo "b released lock1"
exit \$RET
!EOF!
PIDB=$!

$SHELL <<!EOF! &
. ./cvmfs_server_util.sh
die() { echo "\$@" >&2; exit 1; }
PS4='c+ '
while ! check_lock $LOCKDIR/shake2; do
  echo "c waiting for shake2"
  sleep 1
done
echo "c waiting for lock2"
acquire_lock $LOCKDIR/lock2 1
echo "c has lock2"
acquire_lock $LOCKDIR/shake4 1
echo "c sent shake4"
while ! check_lock $LOCKDIR/shake5; do
  echo "c waiting for shake5"
  sleep 1
done
release_lock $LOCKDIR/shake4
release_lock $LOCKDIR/lock2
echo "c released lock2"
!EOF!
PIDC=$!

# set a timeout
MYPID=$$
(sleep 30; set -x; rm -f $LOCKDIR; kill $PIDB; kill $PIDC; kill $MYPID) 2>/dev/null &
PIDTIMEOUT=$!

while ! check_lock $LOCKDIR/shake1; do
  echo "a waiting for shake1"
  sleep 1
done
# b should have lock1 & lock2 now; tell c to wait for lock2
acquire_lock $LOCKDIR/shake2 1
echo "a sent shake2"
# make sure it has enough time to start waiting
echo "a sleeping 3 seconds"
sleep 3
# make sure c doesn't have lock2 yet
if check_lock $LOCKDIR/shake4; then
  echo "shake5 is unexpectedly held too early" >&2
  RET=$(($RET + 1))
fi
# tell b to release lock2
acquire_lock $LOCKDIR/shake3 1
echo "a sent shake3"
release_lock $LOCKDIR/shake2
# wait for b's acknowledgement
while ! check_lock $LOCKDIR/shake4; do
  echo "a waiting for shake4"
  sleep 1
done
release_lock $LOCKDIR/shake3
acquire_lock $LOCKDIR/shake5 1
echo "a sent shake5"

wait $PIDB
RET=$(($RET + $?))

wait $PIDC
RET=$(($RET + $?))

release_lock $LOCKDIR/shake5

kill $PIDTIMEOUT 2>/dev/null
wait $PIDTIMEOUT 2>/dev/null

rm -rf $LOCKDIR

FAILS=$(($FAILS + $RET))

exit $FAILS
