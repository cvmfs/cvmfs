#!/bin/sh

# This script spawns a virtual machine of a specific platform type on ibex and
# runs the associated test cases on this machine

# Configuration for cloud access
# Example:
# EC2_ACCESS_KEY="..."
# EC2_SECRET_KEY="..."
# EC2_ENDPOINT="..."
# EC2_KEY="..."
# EC2_INSTANCE_TYPE="..."
# EC2_KEY_LOCATION="/home/.../ibex_key.pem"

# internal script configuration
script_location=$(dirname $(readlink --canonicalize $0))
reachability_timeout=60  # * 10 seconds
accessibility_timeout=60 # * 10 seconds

# static information (check also remote_setup.sh and remote_run.sh)
cvmfs_workspace="/tmp/cvmfs-test-workspace"
cvmfs_source_directory="${cvmfs_workspace}/cvmfs-source"
cvmfs_setup_log="${cvmfs_workspace}/setup.log"
cvmfs_run_log="${cvmfs_workspace}/run.log"
cvmfs_test_log="${cvmfs_workspace}/test.log"
cvmfs_unittest_log="${cvmfs_workspace}/unittest.log"

# global variables for external script parameters
platform_run_script=""
platform_setup_script=""
server_package=""
client_package=""
old_client_package="notprovided"
keys_package=""
source_tarball=""
unittest_package=""
ec2_config="ec2_config.sh"
ami_name=""
log_destination="."

# global variables (get filled by spawn_virtual_machine)
ip_address=""
instance_id=""


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


die() {
  local msg=$1
  echo $msg
  exit 103
}


usage() {
  local msg=$1

  echo "$msg"
  echo
  echo "Mandatory options:"
  echo " -s <cvmfs server package>  CernVM-FS server package to be tested"
  echo " -c <cvmfs client package>  CernVM-FS client package to be tested"
  echo " -t <cvmfs source tarball>  CernVM-FS sources containing associated tests"
  echo " -g <cvmfs tests package>   CernVM-FS unit tests package"
  echo " -k <cvmfs keys package>    CernVM-FS public keys package"
  echo " -b <setup script>          platform specific setup script (inside the tarball)"
  echo " -r <run script>            platform specific test script (inside the tarball)"
  echo " -a <AMI name>              the virtual machine image to spawn"
  echo
  echo "Optional parameters:"
  echo " -e <EC2 config file>       local location of the ec2_config.sh file"
  echo " -o <old client package>    CernVM-FS client package to be hotpatched on"
  echo " -d <results destination>   Directory to store final test session logs"
  echo
  echo "You must provide http addresses for all packages and tar balls. They will"
  echo "be downloaded and executed to test CVMFS on the specified platform"

  exit 1
}


check_retcode() {
  local retcode=$1

  if [ $retcode -ne 0 ]; then
    echo "fail"
  else
    echo "okay"
  fi

  return $retcode
}


check_timeout() {
  timeout_state=$1
  [ $timeout_state -ne 0 ]
  check_retcode $?
}


spawn_virtual_machine() {
  local ami=$1

  local retcode

  # spawn the virtual machine on ibex
  echo -n "spawning virtual machine from $ami... "
  local spawn_results
  spawn_results=$(${script_location}/instance_handler.py spawn                 \
                                         --access-key       $EC2_ACCESS_KEY    \
                                         --secret-key       $EC2_SECRET_KEY    \
                                         --cloud-endpoint   $EC2_ENDPOINT      \
                                         --key              $EC2_KEY           \
                                         --instance-type    $EC2_INSTANCE_TYPE \
                                         --ami              $ami)
  retcode=$?
  instance_id=$(echo $spawn_results | awk '{print $1}')
  ip_address=$(echo $spawn_results | awk '{print $2}')

  check_retcode $retcode
}


wait_for_virtual_machine() {
  local ip=$1

  # wait for the virtual machine to respond to pings
  echo -n "waiting for IP ($ip) to become reachable... "
  local timeout=$reachability_timeout
  while [ $timeout -gt 0 ] && ! ping -c 1 $ip > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if ! check_timeout $timeout; then return 1; fi

  # wait for the virtual machine to become accessible via ssh
  echo -n "waiting for VM ($ip) to become accessible... "
  timeout=$accessibility_timeout
  while [ $timeout -gt 0 ] &&                                      \
        ! ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no     \
                                   -o UserKnownHostsFile=/dev/null \
                                   -o LogLevel=ERROR               \
                                   -o BatchMode=yes                \
              root@$ip 'echo hallo' > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if ! check_timeout $timeout; then return 1; fi
}


tear_down_virtual_machine() {
  local instance=$1

  echo -n "tearing down virtual machine instance $instance... "
  local teardown_results
  teardown_results=$(${script_location}/instance_handler.py terminate          \
                                         --access-key       $EC2_ACCESS_KEY    \
                                         --secret-key       $EC2_SECRET_KEY    \
                                         --cloud-endpoint   $EC2_ENDPOINT      \
                                         --instance-id      $instance)
  check_retcode $?
}


run_script_on_virtual_machine() {
  local ip=$1
  local script_path=$2
  shift 2

  ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no     \
                           -o UserKnownHostsFile=/dev/null \
                           -o LogLevel=ERROR               \
                           -o BatchMode=yes                \
      root@$ip 'cat | bash /dev/stdin' $@ < $script_path
}


retrieve_file_from_virtual_machine() {
  local ip=$1
  local file_path=$2
  local dest_path=$3

  scp -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no \
      root@${ip}:${file_path} ${dest_path} > /dev/null 2>&1
}


setup_virtual_machine() {
  local ip=$1

  local remote_setup_script
  remote_setup_script="${script_location}/remote_setup.sh"

  echo -n "setting up VM ($ip) for CernVM-FS test suite... "
  run_script_on_virtual_machine $ip $remote_setup_script \
      -s $server_package                                 \
      -c $client_package                                 \
      -o $old_client_package                             \
      -t $source_tarball                                 \
      -g $unittest_package                               \
      -k $keys_package                                   \
      -r $platform_setup_script
  check_retcode $?
  if [ $? -ne 0 ]; then
    handle_test_failure $ip
    return 1
  fi

  echo -n "giving the dust time to settle... "
  sleep 15
  echo "done"
}


run_test_cases() {
  local ip=$1

  local retcode
  local log_files
  local remote_run_script
  remote_run_script="${script_location}/remote_run.sh"

  echo -n "running test cases on VM ($ip)... "
  run_script_on_virtual_machine $ip $remote_run_script \
      -r $platform_run_script
  check_retcode $?

  if [ $? -ne 0 ]; then
    handle_test_failure $ip
  fi
}


handle_test_failure() {
  local ip=$1

  get_test_results $ip

  echo "at least one test case failed... skipping destructions of VM!"
  exit 100
}


get_test_results() {
  local ip=$1
  local retval_run_log
  local retval_test_log
  local retval_setup_log
  local retval_unittest_log

  echo -n "retrieving test results... "
  retrieve_file_from_virtual_machine                  \
      $ip                                             \
      $cvmfs_test_log                                 \
      ${log_destination}/$(basename $cvmfs_test_log)
  retval_test_log=$?
  retrieve_file_from_virtual_machine                  \
      $ip                                             \
      $cvmfs_run_log                                  \
      ${log_destination}/$(basename $cvmfs_run_log)
  retval_run_log=$?
  retrieve_file_from_virtual_machine                  \
      $ip                                             \
      $cvmfs_setup_log                                \
      ${log_destination}/$(basename $cvmfs_setup_log)
  retval_setup_log=$?
  retrieve_file_from_virtual_machine                  \
      $ip                                             \
      $cvmfs_unittest_log                             \
      ${log_destination}/$(basename $cvmfs_unittest_log)
  retval_unittest_log=$?

  [ $retval_test_log     -eq 0 ] && \
  [ $retval_run_log      -eq 0 ] && \
  [ $retval_setup_log    -eq 0 ] && \
  [ $retval_unittest_log -eq 0 ]
  check_retcode $?
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


while getopts "r:b:s:c:o:t:g:k:e:a:d:" option; do
  case $option in
    r)
      platform_run_script=$OPTARG
      ;;
    b)
      platform_setup_script=$OPTARG
      ;;
    s)
      server_package=$OPTARG
      ;;
    c)
      client_package=$OPTARG
      ;;
    o)
      old_client_package=$OPTARG
      ;;
    t)
      source_tarball=$OPTARG
      ;;
    g)
      unittest_package=$OPTARG
      ;;
    k)
      keys_package=$OPTARG
      ;;
    e)
      ec2_config=$OPTARG
      ;;
    a)
      ami_name=$OPTARG
      ;;
    d)
      log_destination=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check if we have all bits and pieces
if [ x$platform_run_script   = "x" ] ||
   [ x$platform_setup_script = "x" ] ||
   [ x$server_package        = "x" ] ||
   [ x$client_package        = "x" ] ||
   [ x$keys_package          = "x" ] ||
   [ x$source_tarball        = "x" ] ||
   [ x$unittest_package      = "x" ] ||
   [ x$ami_name              = "x" ]; then
  usage "Missing parameter(s)"
fi

# load EC2 configuration
. $ec2_config

# spawn the virtual machine image, run the platform specific setup script
# on it, wait for the spawning and setup to be complete and run the actual
# test suite on the VM.
spawn_virtual_machine     $ami_name    || die "Aborting..."
wait_for_virtual_machine  $ip_address  || die "Aborting..."
setup_virtual_machine     $ip_address  || die "Aborting..."
wait_for_virtual_machine  $ip_address  || die "Aborting..."
run_test_cases            $ip_address  || die "Aborting..."
get_test_results          $ip_address  || die "Aborting..."
tear_down_virtual_machine $instance_id || die "Aborting..."

echo "all done"
