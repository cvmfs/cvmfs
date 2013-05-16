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


rpm_name_string() {
  local rpm_file=$1
  echo $(rpm -qp --queryformat '%{NAME}' $rpm_file)
}


check_yum_response() {
  local retcode=$1
  local yum_output=$2

  if [ $retcode -ne 0 ]; then
    echo "fail"
    echo "Yum said:"
    echo $yum_output
    exit 102
  else
    echo "done"
  fi
}


install_rpm() {
  local rpm_file=$1
  local yum_output
  local rpm_name=$(rpm_name_string $rpm_file)

  # check if the given rpm is already installed
  if rpm -q $rpm_name > /dev/null 2>&1; then
    echo "RPM '$rpm_name' is already installed"
    exit 101
  fi

  # install the RPM
  echo -n "Installing RPM '$rpm_name' ... "
  yum_output=$(sudo yum -y install --nogpgcheck $rpm_file 2>&1)
  check_yum_response $? "$yum_output"
}


install_from_repo() {
  local package_name=$1
  local yum_output

  # install package from repository
  echo -n "Installing Package '$package_name' ... "
  yum_output=$(sudo yum -y install $package_name 2>&1)
  check_yum_response $? "$yum_output"
}


uninstall_rpm() {
  local rpm_file=$1
  local yum_output
  local rpm_name=$(rpm_name_string $rpm_file)

  echo -n "Uninstalling RPM '$rpm_name' ... "
  yum_output=$(sudo yum -y erase $rpm_name 2>&1)
  check_yum_response $? "$yum_output"
}


attach_user_group() {
  local groupname=$1
  local username
  local original_group

  # add the group to the user's list of groups
  username=$(id --user --name)
  sudo /usr/sbin/usermod -a -G $groupname $username || return 1

  # Hack! make the new group effective without re-login
  # See: superuser.com/questions/272061/reload-a-linux-users-group-assignments-without-logging-out
  original_group=$(id --group --name)
  newgrp $groupname      || return 2
  newgrp $original_group || return 3

  # check if the group is now visible in groups
  groups | grep -q $groupname || return 4
}


die() {
  local msg=$1
  echo $msg
  exit 103
}
