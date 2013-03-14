#!/bin/sh

#
# Common functionality for cloud platform test execution engine
# After sourcing this file the following variables are set:
#
#    SERVER_PACKAGE     location of the CernVM-FS server package to install
#    CLIENT_PACKAGE     location of the CernVM-FS client package to install
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

#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#

install_rpm() {
  local rpm_name=$1
  shift
  echo -n "Installing RPM $rpm_name ... "
  local ret=$(sudo rpm -ivh $1 $@ 2>&1)
  if [ $? -ne 0 ]; then
    echo "fail"
    echo "RPM said:"
    echo $ret
  else
    echo "done"
  fi
}
