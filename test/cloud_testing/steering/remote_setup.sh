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
  echo "-g <cvmfs tests package>   CernVM-FS unit tests package"
  echo "-k <cvmfs keys package>    CernVM-FS public keys package"
  echo "-r <setup script>          platform specific script (inside the tarball)"
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
  local download_output=

  local dl_bin=""
  if which wget > /dev/null 2>&1; then
    dl_bin="wget --no-check-certificate"
  elif which curl > /dev/null 2>&1; then
    dl_bin="curl --insecure --silent --remote-name"
  else
    echo "didn't find wget or curl."
    exit 1
  fi

  echo -n "downloading $url ... "
  download_output=$($dl_bin $url 2>&1)

  if [ $? -ne 0 ]; then
    echo "fail"
    echo "downloader said:"
    echo $download_output
    exit 2
  else
    echo "done"
  fi
}

export LC_ALL=C

# static information (check also remote_run.sh and run.sh)
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

# parameterized information
platform_script=""
platform_script_path=""
server_package=""
client_package=""
keys_package=""
source_tarball=""
unittest_package=""
test_username="sftnight"

# RHEL (SLC) requires a tty for sudo... work around that
if [ $(id -u) -eq 0 ]; then
  if ! cat /etc/sudoers | grep -q "Defaults:root !requiretty"; then
    echo "Defaults:root !requiretty" | tee --append /etc/sudoers > /dev/null 2>&1
  fi
fi

# create a workspace
sudo rm -fR $cvmfs_workspace > /dev/null 2>&1
mkdir -p $cvmfs_workspace
if [ $? -ne 0 ]; then
  echo "failed to create workspace $cvmfs_workspace"
  exit 3
fi
cd $cvmfs_workspace

# create log files
for log_file in $all_log_files; do
  touch $log_file
done

# from now on everything is logged to the logfile
# Note: the only output of this script is the absolute path to the generated
#       log files
exec &> $cvmfs_setup_log

# read parameters
while getopts "r:s:c:t:g:k:p:u:" option; do
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
    g)
      unittest_package=$OPTARG
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

# check that sudo works as expected
sudo echo "testing sudo..." || exit 2

# check if we have all bits and pieces
if [ x$platform_script  = "x" ] ||
   [ x$server_package   = "x" ] ||
   [ x$client_package   = "x" ] ||
   [ x$keys_package     = "x" ] ||
   [ x$source_tarball   = "x" ] ||
   [ x$unittest_package = "x" ]; then
  usage "Missing parameter(s)"
fi

# create test user account if necessary
id $test_username > /dev/null 2>&1
if [ $? -ne 0 ]; then
  sudo /usr/sbin/useradd --create-home -s /bin/bash $test_username
  if [ $? -ne 0 ]; then
    echo "cannot create user account $test_username"
    exit 4
  fi
  echo "$test_username ALL = NOPASSWD: ALL"  | sudo tee --append /etc/sudoers
  echo "Defaults:$test_username !requiretty" | sudo tee --append /etc/sudoers
fi

# download the needed packages
echo "downloading packages..."
download $server_package
download $client_package
download $keys_package
download $source_tarball
download $unittest_package

# get local file path of downloaded files
server_package=$(readlink --canonicalize $(basename $server_package))
client_package=$(readlink --canonicalize $(basename $client_package))
keys_package=$(readlink --canonicalize $(basename $keys_package))
source_tarball=$(readlink --canonicalize $(basename $source_tarball))
unittest_package=$(readlink --canonicalize $(basename $unittest_package))

# extract the source tarball
extract_location=$(tar -tzf $source_tarball | head -n1)
echo -n "extracting the CernVM-FS source file into $extract_location... "
tar_output=$(tar -xzf $source_tarball)
if [ $? -ne 0 ] || [ ! -d $extract_location ]; then
  echo "fail"
  echo "tar said:"
  echo $tar_output
  exit 5
else
  echo "done"
fi
echo -n "renaming $extract_location to generic name $cvmfs_source_directory... "
mv $(readlink --canonicalize $extract_location) $cvmfs_source_directory > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "fail"
  exit 6
else
  echo "done"
fi

# chown the source tree to allow $test_username to work with it
sudo chown -R $test_username:$test_username $cvmfs_workspace

# find the platform specific script
if [ x$platform_script_path = "x" ]; then
  platform_script_path=${cvmfs_source_directory}/test/cloud_testing/platforms
fi
platform_script_abs=${platform_script_path}/${platform_script}
if [ ! -f $platform_script_abs ]; then
  echo "platform specific script $platform_script not found here:"
  echo $platform_script_abs
  exit 7
fi

# run the platform specific script to perform platform specific test setups
echo "running platform specific script $platform_script... "
sudo -H -E -u $test_username sh $platform_script_abs -s $server_package           \
                                                     -c $client_package           \
                                                     -g $unittest_package         \
                                                     -k $keys_package             \
                                                     -t $cvmfs_source_directory   \
                                                     -l $cvmfs_test_log           \
                                                     -u $cvmfs_unittest_log       \
                                                     -m $cvmfs_migrationtest_log
