#!/bin/bash

portable_dirname() {
  if [ "x$(uname -s)" = "xDarwin" ]; then
    echo $(dirname $(/usr/local/bin/greadlink --canonicalize $1))
  else
    echo $(dirname $(readlink --canonicalize $1))
  fi
}

script_location=$(portable_dirname $0)
. ${script_location}/common.sh

#
# Common functionality for cloud platform test execution engine (test setup)
# After sourcing this file the following variables are set:
#
#  SERVER_PACKAGE        location of the CernVM-FS server package to install
#  CLIENT_PACKAGE        location of the CernVM-FS client package to install
#  FUSE3_PACKAGE         location of the libcvmfs_fuse3 package
#  DEVEL_PACKAGE         location of the CernVM-FS devel package to install
#  CONFIG_PACKAGES       location of the CernVM-FS config packages
#  SOURCE_DIRECTORY      location of the CernVM-FS sources forming above packages
#  UNITTEST_PACKAGE      location of the CernVM-FS unit test package
#  SHRINKWRAP_PACKAGE    location of the CernVM-FS shrinkwrap package
#  GATEWAY_PACKAGE       location of the CernVM-FS gateway package
#  LOG_DIRECTORY         location of the test log files to be created
#  DUCC_PACKAGE          location of the DUCC package
#

SERVER_PACKAGE=""
CLIENT_PACKAGE=""
FUSE3_PACKAGE=""
DEVEL_PACKAGE=""
UNITTEST_PACKAGE=""
SHRINKWRAP_PACKAGE=""
GATEWAY_PACKAGE=""
CONFIG_PACKAGES=""
SOURCE_DIRECTORY=""
LOG_DIRECTORY=""
DUCC_PACKAGE=""
SERVICE_CONTAINER=""

usage() {
  local msg=$1

  echo "$msg"
  echo
  echo "Mandatory options:"
  echo " -t <cvmfs source tree>      CernVM-FS source tree location"
  echo " -s <cvmfs server package>   CernVM-FS server package to be tested"
  echo " -c <cvmfs client package>   CernVM-FS client package to be tested"
  echo " -f <cvmfs fuse3 package>    CernVM-FS fuse3 package to be tested"
  echo " -l <test log directory>     destination for log file generation"
  echo " -d <cvmfs devel package>    CernVM-FS devel package to be tested"
  echo " -k <cvmfs config packages>  CernVM-FS config packages to be tested"
  echo " -g <cvmfs unittest package> CernVM-FS unittest package to be tested"
  echo " -p <shrinkwrap package>     CernVM-FS shrinkwrap package to be tested"
  echo " -w <gateway package>        CernVM-FS gateway to be tested"

  exit 1
}

# parse script parameters (same for all platforms)
while getopts "s:c:d:k:t:g:l:w:n:p:f:D:C:" option; do
  case $option in
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
      ;;
    f)
      FUSE3_PACKAGE=$OPTARG
      ;;
    d)
      DEVEL_PACKAGE=$OPTARG
      ;;
    k)
      CONFIG_PACKAGES="$OPTARG"
      ;;
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    g)
      UNITTEST_PACKAGE=$OPTARG
      ;;
    p)
      SHRINKWRAP_PACKAGE=$OPTARG
      ;;
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    w)
      GATEWAY_PACKAGE=$OPTARG
      ;;
    D)
      DUCC_PACKAGE=$OPTARG
      ;;
    C)
      SERVICE_CONTAINER=$OPTARG
      ;;
    n)
      echo "WARNING: the -n parameter is obsolete"
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ "x$SOURCE_DIRECTORY"      = "x" ] ||
   [ "x$LOG_DIRECTORY"         = "x" ] ||
   [ "x$CLIENT_PACKAGE"        = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi
if [ "x$(uname -s)" != "xDarwin" ]; then
    if [ "x$SERVER_PACKAGE"        = "x" ] ||
       [ "x$CONFIG_PACKAGES"       = "x" ] ||
       [ "x$UNITTEST_PACKAGE"      = "x" ] ||
       [ "x$SHRINKWRAP_PACKAGE"    = "x" ] ||
       [ "x$DEVEL_PACKAGE"         = "x" ]; then
      echo "missing parameter(s), cannot run platform dependent test script"
      exit 200
    fi
fi

# check that the script is running under the correct user account
if [ $(id -u -n) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi

echo "Platform environment:"
env
echo "Hostname is $(hostname)"
