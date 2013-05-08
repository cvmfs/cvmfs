#!/bin/sh

#
# Common functionality for cloud platform test execution engine
# After sourcing this file the following variables are set:
#
#    SERVER_PACKAGE     location of the CernVM-FS server package to install
#    CLIENT_PACKAGE     location of the CernVM-FS client package to install
#    KEYS_PACKAGE       location of the CernVM-FS public keys package
#    SOURCE_DIRECTORY   location of the CernVM-FS sources forming above packages
#    TEST_LOGFILE       location of the test logfile to be used
#

SERVER_PACKAGE=""
CLIENT_PACKAGE=""
KEYS_PACKAGE=""
SOURCE_DIRECTORY=""
TEST_LOGFILE=""

# parse script parameters (same for all platforms)
while getopts "s:c:k:t:l:" option; do
  case $option in
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
      ;;
    k)
      KEYS_PACKAGE=$OPTARG
      ;;
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    l)
      TEST_LOGFILE=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SERVER_PACKAGE   = "x" ] ||
   [ x$CLIENT_PACKAGE   = "x" ] ||
   [ x$KEYS_PACKAGE     = "x" ] ||
   [ x$SOURCE_DIRECTORY = "x" ] ||
   [ x$TEST_LOGFILE     = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi

# check that the script is running under the correct user account
if [ $(id --user --name) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi

#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#

install_rpm() {
  local rpm_name=$1
  local rpm_output
  shift 1

  # check if one of the given rpms is already installed
  for rpm in $@; do
    local rpm_package=$(basename $rpm .rpm)
    if rpm -q $rpm_package > /dev/null 2>&1; then
      echo "RPM '$rpm_name' is already installed"
      exit 101
    fi
  done

  # install the RPM
  echo -n "Installing RPM '$rpm_name' ... "
  yum_output=$(sudo yum -y install $@ 2>&1)
  if [ $? -ne 0 ]; then
    echo "fail"
    echo "Yum said:"
    echo $yum_output
    exit 102
  else
    echo "done"
  fi
}


die() {
  local msg=$1
  echo $msg
  exit 103
}
