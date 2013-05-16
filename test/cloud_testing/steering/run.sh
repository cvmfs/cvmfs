#!/bin/sh

# This script spawns a virtual machine of a specific platform type on ibex and
# runs the associated test cases on this machine

# internal script configuration
script_location=$(dirname $(readlink --canonicalize $0))
reachability_timeout=60  # * 10 seconds
accessibility_timeout=60 # * 10 seconds

# Load configuration
#
# Example:
# EC2_ACCESS_KEY="..."
# EC2_SECRET_KEY="..."
# EC2_ENDPOINT="..."
# EC2_KEY="..."
# EC2_INSTANCE_TYPE="..."
# EC2_KEY_LOCATION="/home/.../ibex_key.pem"
. ${script_location}/ec2_config.sh

# global variables for external script parameters
platform_script=""
server_package=""
client_package=""
keys_package=""
source_tarball=""
ami_name=""

# global variables (get filled by spawn_virtual_machine)
ip_address=""
instance_id=""

# global variables (get filled by setup_and_run_test_cases)
remote_run_log_file=""
remote_test_log_file=""


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
  echo "-s <cvmfs server package>  CernVM-FS server package to be tested"
  echo "-c <cvmfs client package>  CernVM-FS client package to be tested"
  echo "-t <cvmfs source tarball>  CernVM-FS sources containing associated tests"
  echo "-k <cvmfs keys package>    CernVM-FS public keys package"
  echo "-r <run script>            platform specific script (inside the tarball)"
  echo "-a <AMI name>              the virtual machine image to spawn on ibex"
  echo
  echo "You must provide http addresses for all packages and tar balls. They will"
  echo "be downloaded and executed to test CVMFS on the specified platform"

  exit 1
}


check_timeout() {
  timeout_state=$1
  if [ $timeout_state -eq 0 ]; then
    echo "fail"
    return 1
  else
    echo "okay"
  fi
}


spawn_virtual_machine() {
  local ami=$1

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
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  else
    instance_id=$(echo $spawn_results | awk '{print $1}')
    ip_address=$(echo $spawn_results | awk '{print $2}')
    echo "okay"
  fi

  # wait for the virtual machine to respond to pings
  echo -n "waiting for IP ($ip_address) to become reachable... "
  local timeout=$reachability_timeout
  while [ $timeout -gt 0 ] && ! ping -c 1 $ip_address > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if ! check_timeout $timeout; then return 1; fi

  # wait for the virtual machine to become accessible via ssh
  echo -n "waiting for VM ($instance_id) to become accessible... "
  timeout=$accessibility_timeout
  while [ $timeout -gt 0 ] && \
        ! ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no root@$ip_address 'echo hallo' > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if ! check_timeout $timeout; then return 1; fi

  # all done...
  return 0
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
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  else
    echo "okay"
  fi
}


setup_and_run_test_cases() {
  local ip=$1

  echo -n "setting up and running test cases on VM ($ip)... "
  log_files=$(ssh -i $EC2_KEY_LOCATION root@$ip 'cat | bash /dev/stdin'        \
                         -s $server_package                                    \
                         -c $client_package                                    \
                         -t $source_tarball                                    \
                         -k $keys_package                                      \
                         -r $platform_script < ${script_location}/remote_run.sh)
  local retcode=$?
  remote_run_log_file=$(echo $log_files  | awk '{print $1}')
  remote_test_log_file=$(echo $log_files | awk '{print $2}')

  if [ $retcode -ne 0 ]; then
    echo "fail"
    return 1
  else
    echo "done"
  fi
}


handle_test_failure() {
  local ip=$1

  echo "handling failure later..."
  echo "run log:  $remote_run_log_file"
  echo "test log: $remote_test_log_file"
}


get_test_results() {
  local ip=$1

  echo "retrieving test results..."
  echo "run log:  $remote_run_log_file"
  echo "test log: $remote_test_log_file"
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


while getopts "r:s:c:t:k:a:" option; do
  case $option in
    r)
      platform_script=$OPTARG
      ;;
    s)
      server_package=$OPTARG
      ;;
    c)
      client_package=$OPTARG
      ;;
    t)
      source_tarball=$OPTARG
      ;;
    k)
      keys_package=$OPTARG
      ;;
    a)
      ami_name=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check if we have all bits and pieces
if [ x$platform_script = "x" ] ||
   [ x$server_package  = "x" ] ||
   [ x$client_package  = "x" ] ||
   [ x$keys_package    = "x" ] ||
   [ x$source_tarball  = "x" ] ||
   [ x$ami_name        = "x" ]; then
  usage "Missing parameter(s)"
fi

# spawn the virtual machine image
spawn_virtual_machine $ami_name
if [ $? -ne 0 ]; then
  echo "Aborting..."
  exit 2
fi

# run the test cases on the spawned virtual machine
setup_and_run_test_cases $ip_address
if [ $? -ne 0 ]; then
  echo "Errors occured during test run. Investigating..."
  handle_test_failure $ip_address
  exit 3
fi

# get the test results
get_test_results $ip_address

# tear down virtual machine after successful test run
tear_down_virtual_machine $instance_id
if [ $? -ne 0 ]; then
  exit 4
fi

# done with the test run
echo "all done"
