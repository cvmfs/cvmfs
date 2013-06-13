#!/bin/sh

#
# Common functionality for cloud platform test execution engine (test setup)
# After sourcing this file the following variables are set:
#
#  SERVER_PACKAGE       location of the CernVM-FS server package to install
#  CLIENT_PACKAGE       location of the CernVM-FS client package to install
#  OLD_CLIENT_PACKAGE   location of an old CernVM-FS client package for hotpatch
#  KEYS_PACKAGE         location of the CernVM-FS public keys package
#  SOURCE_DIRECTORY     location of the CernVM-FS sources forming above packages
#  UNITTEST_PACKAGE     location of the CernVM-FS unit test package
#  TEST_LOGFILE         location of the test logfile to be used
#  UNITTEST_LOGFILE     location of the unit test logfile to be used
#
# NOTE: the OLD_CLIENT_PACKAGE is not mandatory and should be checked for exist-
#       ance before usage
#

SERVER_PACKAGE=""
CLIENT_PACKAGE=""
OLD_CLIENT_PACKAGE=""
UNITTEST_PACKAGE=""
KEYS_PACKAGE=""
SOURCE_DIRECTORY=""
TEST_LOGFILE=""
UNITTEST_LOGFILE=""

# parse script parameters (same for all platforms)
while getopts "s:c:o:k:t:g:l:u:" option; do
  case $option in
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
      ;;
    o)
      OLD_CLIENT_PACKAGE=$OPTARG
      ;;
    k)
      KEYS_PACKAGE=$OPTARG
      ;;
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    g)
      UNITTEST_PACKAGE=$OPTARG
      ;;
    l)
      TEST_LOGFILE=$OPTARG
      ;;
    u)
      UNITTEST_LOGFILE=$OPTARG
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
   [ x$UNITTEST_PACKAGE = "x" ] ||
   [ x$TEST_LOGFILE     = "x" ] ||
   [ x$UNITTEST_LOGFILE = "x" ]; then
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


deb_name_string() {
  local deb_file=$1
  echo $(dpkg --info $deb_file | grep " Package: " | sed 's/ Package: //')
}


check_package_manager_response() {
  local retcode=$1
  local pkg_mgr_name="$2"
  local pkg_mgr_output="$3"

  if [ $retcode -ne 0 ]; then
    echo "fail"
    echo "$pkg_mgr_name said:"
    echo $package_mgr_output
    exit 102
  else
    echo "done"
  fi

  return $retcode
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
  check_package_manager_response $? "Yum" "$yum_output"
}


install_deb() {
  local deb_file=$1
  local deb_output
  local deb_name=$(deb_name_string $deb_file)

  # install DEB package
  echo -n "Installing DEB package '$deb_name' ... "
  deb_output=$(sudo gdebi --non-interactive --quiet $deb_file)
  check_package_manager_response $? "DPKG" "$deb_output"
}


install_from_repo() {
  local package_name=$1
  local pkg_mgr
  local pkg_mgr_output

  # find out which package manager to use
  if which apt-get > /dev/null 2>&1; then
    pkg_mgr="apt-get"
  else
    pkg_mgr="yum"
  fi

  # install package from repository
  echo -n "Installing Package '$package_name' ... "
  pkg_mgr_output=$(sudo $pkg_mgr -y install $package_name 2>&1)
  check_package_manager_response $? $pkg_mgr "$pkg_mgr_output"
}


attach_user_group() {
  local groupname=$1
  local username

  # add the group to the user's list of groups
  username=$(id --user --name)
  sudo /usr/sbin/usermod -a -G $groupname $username || return 1
}


die() {
  local msg="$1"
  echo $msg
  exit 103
}
