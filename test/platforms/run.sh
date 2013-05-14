#!/bin/bash

#
# This script is designed to be as platform independent as possible. It down-
# loads the CernVM-FS packages provided by the test supervisor (i.e. Electric
# Commander) and executes a platform specific test initialization script found
# in the CernVM-FS repository
#

usage() {
  local error_msg=$1

  echo "$error_msg"
  echo
  echo "Mandatory options:"
  echo "-s <cvmfs server package>  CernVM-FS server package to be tested"
  echo "-c <cvmfs client package>  CernVM-FS client package to be tested"
  echo "-t <cvmfs source tarball>  CernVM-FS sources containing associated tests"
  echo "-k <cvmfs keys package>    CernVM-FS public keys package"
  echo "-r <run script>            platform specific script (inside the tarball)"
  echo
  echo "Optional parameters:"
  echo "-p <platform path>         custom search path for platform specific script"
  echo "-u <user name>             user name to use for test run"
  echo
  echo "You must provide http addresses for all packages and tar balls. They will"
  echo "be downloaded and executed to test CVMFS on various platforms"

  exit 1
}

download() {
  local url=$1
  local download_output=$(wget $url 2>&1)

  echo -n "downloading $url ... "

  if [ $? -ne 0 ]; then
    echo "fail"
    echo "wget said:"
    echo $download_output
    exit 2
  else
    echo "done"
  fi
}

# static information
run_logfile=run.log
test_logfile=test.log

platform_script=""
platform_script_path=""
server_package=""
client_package=""
keys_package=""
source_tarball=""
test_username="sftnight"

# create a workspace
workspace=$(mktemp -d)
cd $workspace

# create log files
touch $run_logfile
touch $test_logfile
run_logfile=$(readlink --canonicalize $run_logfile)
test_logfile=$(readlink --canonicalize $test_logfile)

# from now on everything is logged to the logfile
# Note: the only output of this script is the absolute path to the generated
#       log files
echo $run_logfile
echo $test_logfile
exec &> $run_logfile

# read parameters
while getopts "r:s:c:t:k:p:u:" option; do
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
if [ x$platform_script = "x" ] ||
   [ x$server_package  = "x" ] ||
   [ x$client_package  = "x" ] ||
   [ x$keys_package    = "x" ] ||
   [ x$source_tarball  = "x" ]; then
  usage "Missing parameter(s)"
fi

# create test user account if necessary
id $test_username > /dev/null
if [ $? -ne 0 ]; then
  /usr/sbin/useradd $test_username
  if [ $? -ne 0 ]; then
    echo "cannot create user account $test_username"
    exit 4
  fi
  echo "$test_username ALL = NOPASSWD: ALL"  | tee --append /etc/sudoers
  echo "Defaults:$test_username !requiretty" | tee --append /etc/sudoers
fi

# adapt sudo configuration for non-tty usage if necessary
if ! cat /etc/sudoers | grep -q "Defaults:root !requiretty"; then
  echo "Defaults:root !requiretty" | tee --append /etc/sudoers
fi

# download the needed packages
echo "downloading packages..."
download $server_package
download $client_package
download $keys_package
download $source_tarball

# get local file path of downloaded files
server_package=$(readlink --canonicalize $(basename $server_package))
client_package=$(readlink --canonicalize $(basename $client_package))
keys_package=$(readlink --canonicalize $(basename $keys_package))
source_tarball=$(readlink --canonicalize $(basename $source_tarball))

# extract the source tarball
source_directory=$(basename $source_tarball .tar.gz | sed 's/_/-/')
echo -n "extracting the CernVM-FS source file into $source_directory... "
tar_output=$(tar -xzf $source_tarball)
if [ $? -ne 0 ] || [ ! -d $source_directory ]; then
  echo "fail"
  echo "tar said:"
  echo $tar_output
  exit 5
else
  echo "done"
fi
source_directory=$(readlink --canonicalize $source_directory)

# chown the source tree to allow $test_username to work with it
chown -R $test_username:$test_username $workspace

# find the platform specific script
if [ x$platform_script_path = "x" ]; then
  platform_script_path=${source_directory}/test/platforms
fi
platform_script_abs=${platform_script_path}/${platform_script}
if [ ! -f $platform_script_abs ]; then
  echo "platform specific script $platform_script not found here:"
  echo $platform_script_abs
  exit 6
fi

# run the platform specific script to perform CernVM-FS tests
echo "running platform specific script $platform_script ..."
touch .running # flag to signal a running test system
sudo -H -u $test_username sh $platform_script_abs -s $server_package   \
                                                  -c $client_package   \
                                                  -k $keys_package     \
                                                  -t $source_directory \
                                                  -l $test_logfile
retval=$?
rm .running

# good bye
echo "all done"
exit $retval
