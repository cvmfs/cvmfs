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
reachability_timeout=1800  # (  30 minutes )
accessibility_timeout=7200 # ( 120 minutes )
keys_package_base_url="https://ecsft.cern.ch/dist/cvmfs/cvmfs-keys"

# static information (check also remote_setup.sh and remote_run.sh)
cvmfs_workspace="/tmp/cvmfs-test-workspace"
cvmfs_source_directory="${cvmfs_workspace}/cvmfs-source"
cvmfs_setup_log="${cvmfs_workspace}/setup.log"
cvmfs_run_log="${cvmfs_workspace}/run.log"
cvmfs_test_log="${cvmfs_workspace}/test.log"
cvmfs_test_s3_log="${cvmfs_workspace}/test_s3.log"
cvmfs_fake_s3_log="${cvmfs_workspace}/fake_s3.log"
cvmfs_unittest_log="${cvmfs_workspace}/unittest.log"
cvmfs_migrationtest_log="${cvmfs_workspace}/migrationtest.log"
all_log_files="$cvmfs_setup_log $cvmfs_run_log $cvmfs_test_log \
$cvmfs_test_s3_log $cvmfs_fake_s3_log $cvmfs_unittest_log $cvmfs_migrationtest_log"

# global variables for external script parameters
testee_url=""
platform=""
platform_run_script=""
platform_setup_script=""
ec2_config="ec2_config.sh"
ami_name=""
log_destination="."
username="root"
userdata=""

# package download locations
server_package=""
client_package=""
keys_package=""
unittest_package=""
source_tarball="source.tar.gz" # will be prepended by ${testee_url} later

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

  echo "Error: $msg"
  echo
  echo "Mandatory options:"
  echo " -u <testee URL>            URL to the nightly build directory to be tested"
  echo " -p <platform name>         name of the platform to be tested"
  echo " -b <setup script>          platform specific setup script (inside the tarball)"
  echo " -r <run script>            platform specific test script (inside the tarball)"
  echo " -a <AMI name>              the virtual machine image to spawn"
  echo
  echo "Optional parameters:"
  echo " -e <EC2 config file>       local location of the ec2_config.sh file"
  echo " -d <results destination>   Directory to store final test session logs"
  echo " -m <ssh user name>         User name to be used for VM login (default: root)"
  echo " -c <cloud init userdata>   User data string to be passed to the new instance"

  exit 1
}


check_retcode() {
  local retcode=$1
  local additional_msg="$2"

  if [ $retcode -ne 0 ]; then
    echo -n "fail"
  else
    echo -n "okay"
  fi
  if [ x"$additional_msg" != x"" ]; then
    echo " ($additional_msg)"
  else
    echo ""
  fi

  return $retcode
}


check_timeout() {
  local timeout_state=$1
  local timeout_start=$2
  local waiting_time=$(( ( $timeout_start - $timeout_state ) / 60 ))

  [ $timeout_state -ne 0 ]
  check_retcode $? "waited $waiting_time minutes"
}


# Reads the package map produced by the nightly build process to map supported
# platforms to their associated packages.
# pkgmap-format (ini-style):
#     [<platform name>]
#     client=<url to client package>
#     server=<url to server package>
#     ...=...
#     [<next platform name>]
#     ...=...
#
# @param pkgmap_url    URL where to find the package map file
# @param platform      the platform name to be searched for
# @param package       the package to be retrieved from the pkgmap
# @return              0 on success (queried package URL through stdout)
read_package_map() {
  local pkgmap_url=$1
  local platform=$2
  local package=$3

  local platform_found=0
  local package_url=""

  for line in $(wget --no-check-certificate --quiet --output-document=- $pkgmap_url); do
    # search the desired platform
    if [ $platform_found -eq 0 ] && [ x"$line" = x"[$platform]" ]; then
      platform_found=1
      continue
    fi

    # when the platform was found, look for the desired package name
    if [ $platform_found -eq 1 ]; then
      # if the next platform starts, we didn't find the desired package
      if echo "$line" | grep -q -e '^\[.*\]$'; then
        break
      fi

      # check for desired package name and possibly return successfully
      if [ x"$(echo "$line" | cut -d= -f1)" = x"$package" ]; then
        package_url=$(echo "$line" | cut -d= -f2)
        break
      fi
    fi
  done

  # check if the desired package URL was found
  if [ x"$package_url" != x"" ]; then
    echo "$package_url"
    return 0
  else
    return 2
  fi
}


spawn_virtual_machine() {
  local ami=$1
  local userdata="$2"

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
                                         --userdata         "$userdata"        \
                                         --ami              $ami)
  retcode=$?
  instance_id=$(echo $spawn_results | awk '{print $1}')
  ip_address=$(echo $spawn_results | awk '{print $2}')

  check_retcode $retcode
}


wait_for_virtual_machine() {
  local ip=$1
  local username=$2

  # wait for the virtual machine to respond to pings
  echo -n "waiting for IP ($ip) to become reachable... "
  local timeout=$reachability_timeout
  while [ $timeout -gt 0 ] && ! ping -c 1 $ip > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 10 ))
  done
  if ! check_timeout $timeout $reachability_timeout; then return 1; fi

  # wait for the virtual machine to become accessible via ssh
  echo -n "waiting for VM ($ip) to become accessible... "
  timeout=$accessibility_timeout
  while [ $timeout -gt 0 ] &&                                      \
        ! ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no     \
                                   -o UserKnownHostsFile=/dev/null \
                                   -o LogLevel=ERROR               \
                                   -o BatchMode=yes                \
              ${username}@${ip} 'echo hallo' > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 10 ))
  done
  if ! check_timeout $timeout $accessibility_timeout; then return 1; fi
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
  local username=$2
  local script_path=$3
  shift 3

  ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no     \
                           -o UserKnownHostsFile=/dev/null \
                           -o LogLevel=ERROR               \
                           -o BatchMode=yes                \
      $username@$ip 'cat | bash /dev/stdin' $@ < $script_path
}


retrieve_file_from_virtual_machine() {
  local ip=$1
  local username=$2
  local file_path=$3
  local dest_path=$4

  scp -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no \
      $username@${ip}:${file_path} ${dest_path} > /dev/null 2>&1
}


setup_virtual_machine() {
  local ip=$1
  local username=$2

  local remote_setup_script
  remote_setup_script="${script_location}/remote_setup.sh"

  echo -n "setting up VM ($ip) for CernVM-FS test suite... "
  run_script_on_virtual_machine $ip $username $remote_setup_script \
      -s $server_package                                           \
      -c $client_package                                           \
      -t $source_tarball                                           \
      -g $unittest_package                                         \
      -k $keys_package                                             \
      -r $platform_setup_script
  check_retcode $?
  if [ $? -ne 0 ]; then
    handle_test_failure $ip $username
    return 1
  fi

  echo -n "giving the dust time to settle... "
  sleep 15
  echo "done"
}


run_test_cases() {
  local ip=$1
  local username=$2

  local retcode
  local log_files
  local remote_run_script
  remote_run_script="${script_location}/remote_run.sh"

  echo -n "running test cases on VM ($ip)... "
  run_script_on_virtual_machine $ip $username $remote_run_script \
      -s $server_package                                         \
      -c $client_package                                         \
      -r $platform_run_script
  check_retcode $?

  if [ $? -ne 0 ]; then
    handle_test_failure $ip $username
  fi
}


handle_test_failure() {
  local ip=$1
  local username=$2

  get_test_results $ip $username

  echo "at least one test case failed... skipping destructions of VM!"
  exit 100
}


get_test_results() {
  local ip=$1
  local username=$2
  local retval=0

  echo -n "retrieving test results... "
  for log_file in $all_log_files; do
    retrieve_file_from_virtual_machine \
        $ip                            \
        $username                      \
        $log_file                      \
        ${log_destination}/$(basename $log_file)
    if [ $? -ne 0 ]; then
      retval=1
    fi
  done
  check_retcode $retval
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


while getopts "r:b:u:p:e:a:d:m:c:" option; do
  case $option in
    r)
      platform_run_script=$OPTARG
      ;;
    b)
      platform_setup_script=$OPTARG
      ;;
    u)
      testee_url=$OPTARG
      ;;
    p)
      platform=$OPTARG
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
    m)
      username=$OPTARG
      ;;
    c)
      userdata="$OPTARG"
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
   [ x$platform              = "x" ] ||
   [ x$testee_url            = "x" ] ||
   [ x$ami_name              = "x" ]; then
  usage "Missing parameter(s)"
fi

# figure out which packages need to be downloaded
client_package=$(read_package_map   ${testee_url}/pkgmap "$platform" 'client'   )
server_package=$(read_package_map   ${testee_url}/pkgmap "$platform" 'server'   )
unittest_package=$(read_package_map ${testee_url}/pkgmap "$platform" 'unittests')
keys_package=$(read_package_map     ${testee_url}/pkgmap "$platform" 'keys'     )

# check if all necessary packages were found
if [ x$server_package        = "x" ] ||
   [ x$client_package        = "x" ] ||
   [ x$keys_package          = "x" ] ||
   [ x$unittest_package      = "x" ]; then
  usage "Incomplete pkgmap file"
fi

# construct the full package URLs
client_package="${testee_url}/${client_package}"
server_package="${testee_url}/${server_package}"
unittest_package="${testee_url}/${unittest_package}"
keys_package="${keys_package_base_url}/${keys_package}"
source_tarball="${testee_url}/${source_tarball}"

# load EC2 configuration
. $ec2_config

# spawn the virtual machine image, run the platform specific setup script
# on it, wait for the spawning and setup to be complete and run the actual
# test suite on the VM.
spawn_virtual_machine     $ami_name "$userdata"  || die "Aborting..."
wait_for_virtual_machine  $ip_address  $username || die "Aborting..."
setup_virtual_machine     $ip_address  $username || die "Aborting..."
wait_for_virtual_machine  $ip_address  $username || die "Aborting..."
run_test_cases            $ip_address  $username || die "Aborting..."
get_test_results          $ip_address  $username || die "Aborting..."
tear_down_virtual_machine $instance_id           || die "Aborting..."

echo "all done"
