#!/bin/bash

#
# This script is designed to be as platform independent as possible. It does
# final preparations to run the platform specific test cases of CernVM-FS and
# invokes a platform dependent script to steer the actual test session
#

usage() {
  local error_msg=$1

  echo "$error_msg"
  echo
  echo "Mandatory options:"
  echo "-r <test script>      platform specific script (inside the cvmfs sources)"
  echo "-s <server package>   CernVM-FS server package to be tested"
  echo "-c <client package>   CernVM-FS client package to be tested"
  echo "-k <config packages>  CernVM-FS configuration packages to be used"
  echo
  echo "Optional parameters:"
  echo "-p <platform path>    custom search path for platform specific script"
  echo "-u <user name>        user name to use for test run"

  exit 1
}

export LC_ALL=C

# static information (check also remote_setup.sh and run.sh)
cvmfs_workspace="/tmp/cvmfs-test-workspace"
cvmfs_source_directory="${cvmfs_workspace}/cvmfs-source"
cvmfs_setup_log="${cvmfs_workspace}/setup.log"
cvmfs_run_log="${cvmfs_workspace}/run.log"
cvmfs_test_log="${cvmfs_workspace}/test.log"
cvmfs_test_s3_log="${cvmfs_workspace}/test_s3.log"
cvmfs_fake_s3_log="${cvmfs_workspace}/fake_s3.log"
cvmfs_unittest_log="${cvmfs_workspace}/unittest.log"
cvmfs_migrationtest_log="${cvmfs_workspace}/migrationtest.log"

# parameterized variables
platform_script=""
platform_script_path=""
test_username="sftnight"
server_package=""
client_package=""
config_packages=""

# from now on everything is logged to the logfile
# Note: the only output of this script is the absolute path to the generated
#       log files
sudo chmod a+w $cvmfs_run_log
exec &> $cvmfs_run_log

# switch to working directory
cd $cvmfs_workspace

# read parameters
while getopts "r:s:c:k:p:u:" option; do
  case $option in
    r)
      platform_script=$OPTARG
      ;;
    s)
      server_package=$(readlink --canonicalize $(basename $OPTARG))
      ;;
    c)
      client_package=$(readlink --canonicalize $(basename $OPTARG))
      ;;
    k)
      config_package_paths=""
      for config_package in $OPTARG; do
        config_package_paths="$(readlink --canonicalize $(basename $config_package)) $config_package_paths"
      done
      config_packages="config_package_paths"
      ;;
    p)
      platform_script_path=$OPTARG
      ;;
    u)
      test_username=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check if we have all bits and pieces
if [ x"$platform_script"  = "x" ] ||
   [ x"$client_package"   = "x" ] ||
   [ x"$server_package"   = "x" ] ||
   [ x"$config_packages"  = "x" ]; then
  usage "Missing parameter(s)"
fi

# check if the needed packages are downloaded
if [ ! -f $server_package ] ||
   [ ! -f $client_package ]; then
  usage "Missing package(s)"
fi

for config_package in $config_packages; do
  [ -f $config_package ] || usage "Missing config package '$config_package'"
done

# export the location of the client, server and config packages
export CVMFS_CLIENT_PACKAGE=$client_package
export CVMFS_SERVER_PACKAGE=$server_package
export CVMFS_CONFIG_PACKAGES="$config_packages"

# change working directory to test workspace
cd $cvmfs_workspace

# find the platform specific script
if [ x$platform_script_path = "x" ]; then
  platform_script_path=${cvmfs_source_directory}/test/cloud_testing/platforms
fi
platform_script_abs=${platform_script_path}/${platform_script}
if [ ! -f $platform_script_abs ]; then
  echo "platform specific script $platform_script not found here:"
  echo $platform_script_abs
  exit 2
fi

# run the platform specific script to perform CernVM-FS tests
echo "running platform specific script $platform_script ..."
sudo -H -E -u $test_username sh $platform_script_abs -t $cvmfs_source_directory\
                                                     -l $cvmfs_test_log        \
                                                     -i $cvmfs_test_s3_log     \
                                                     -j $cvmfs_fake_s3_log     \
                                                     -s $server_package        \
                                                     -c $client_package        \
                                                     -u $cvmfs_unittest_log    \
                                                     -m $cvmfs_migrationtest_log
