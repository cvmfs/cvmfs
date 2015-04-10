#!/bin/sh

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

#
# Common functionality for cloud platform test execution engine (test setup)
# After sourcing this file the following variables are set:
#
#  SERVER_PACKAGE        location of the CernVM-FS server package to install
#  CLIENT_PACKAGE        location of the CernVM-FS client package to install
#  CONFIG_PACKAGES       location of the CernVM-FS config packages
#  SOURCE_DIRECTORY      location of the CernVM-FS sources forming above packages
#  UNITTEST_PACKAGE      location of the CernVM-FS unit test package
#  LOG_DIRECTORY         location of the test log files to be created
#

SERVER_PACKAGE=""
CLIENT_PACKAGE=""
UNITTEST_PACKAGE=""
CONFIG_PACKAGES=""
SOURCE_DIRECTORY=""
LOG_DIRECTORY=""

# parse script parameters (same for all platforms)
while getopts "s:c:k:t:g:l:" option; do
  case $option in
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
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
    echo $pkg_mgr_output
    exit 102
  else
    echo "done"
  fi

  return $retcode
}


install_rpm() {
  local rpm_files="$1"
  local yum_output

  for this_rpm in $rpm_files; do
    local rpm_name=$(rpm_name_string $this_rpm)

    # check if the given rpm is already installed
    if rpm -q $rpm_name > /dev/null 2>&1; then
      echo "RPM '$rpm_name' is already installed"
      exit 101
    fi

    # install the RPM
    echo -n "Installing RPM '$rpm_name' ... "
    yum_output=$(sudo yum -y install --nogpgcheck $this_rpm 2>&1)
    check_package_manager_response $? "Yum" "$yum_output"
  done
}


install_deb() {
  local deb_files="$1"
  local deb_output

  for this_deb in $deb_files; do
    local deb_name=$(deb_name_string $this_deb)

    # install DEB package
    echo -n "Installing DEB package '$deb_name' ... "
    deb_output=$(sudo gdebi --non-interactive --quiet $this_deb)
    check_package_manager_response $? "DPKG" "$deb_output"
  done
}


install_from_repo() {
  local package_names="$1"
  local pkg_mgr
  local pkg_mgr_output

  # find out which package manager to use
  if which apt-get > /dev/null 2>&1; then
    pkg_mgr="apt-get"
  else
    pkg_mgr="yum"
  fi

  # install package from repository
  echo -n "Installing Packages '$package_names' ... "
  pkg_mgr_output=$(sudo $pkg_mgr -y install $package_names 2>&1)
  check_package_manager_response $? $pkg_mgr "$pkg_mgr_output"
}


install_ruby_gem() {
  local gem_name=$1
  local pkg_mgr_name="gem"
  local pkg_mgr_output=""

  echo -n "Installing Ruby gem '$gem_name' ... "
  pkg_mgr_output=$(sudo gem install $gem_name 2>&1)
  check_package_manager_response $? $pkg_mgr_name "$pkg_mgr_output"
}


attach_user_group() {
  local groupname=$1
  local username

  # add the group to the user's list of groups
  username=$(id --user --name)
  sudo /usr/sbin/usermod -a -G $groupname $username || return 1
}


set_nofile_limit() {
  local limit_value=$1
  echo "*    hard nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "*    soft nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "root hard nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "root soft nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
}

echo "Hostname is $(hostname)"
