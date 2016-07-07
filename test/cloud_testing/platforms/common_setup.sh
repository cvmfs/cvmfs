#!/bin/bash

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

#
# Common functionality for cloud platform test execution engine (test setup)
# After sourcing this file the following variables are set:
#
#  SERVER_PACKAGE        location of the CernVM-FS server package to install
#  CLIENT_PACKAGE        location of the CernVM-FS client package to install
#  DEVEL_PACKAGE         location of the CernVM-FS devel package to install
#  CONFIG_PACKAGES       location of the CernVM-FS config packages
#  SOURCE_DIRECTORY      location of the CernVM-FS sources forming above packages
#  UNITTEST_PACKAGE      location of the CernVM-FS unit test package
#  LOG_DIRECTORY         location of the test log files to be created
#

SERVER_PACKAGE=""
CLIENT_PACKAGE=""
DEVEL_PACKAGE=""
UNITTEST_PACKAGE=""
CONFIG_PACKAGES=""
SOURCE_DIRECTORY=""
LOG_DIRECTORY=""

# parse script parameters (same for all platforms)
while getopts "s:c:d:k:t:g:l:" option; do
  case $option in
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
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
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ "x$SERVER_PACKAGE"        = "x" ] ||
   [ "x$CLIENT_PACKAGE"        = "x" ] ||
   [ "x$DEVEL_PACKAGE"         = "x" ] ||
   [ "x$CONFIG_PACKAGES"       = "x" ] ||
   [ "x$SOURCE_DIRECTORY"      = "x" ] ||
   [ "x$UNITTEST_PACKAGE"      = "x" ] ||
   [ "x$LOG_DIRECTORY"         = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi

# check that the script is running under the correct user account
if [ $(id --user --name) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi

echo "Hostname is $(hostname)"
