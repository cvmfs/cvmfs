#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

echo "enabling epel yum repository..."
install_from_repo epel-release

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $LIBS_PACKAGE
install_rpm $FUSE3_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $UNITTEST_PACKAGE
install_rpm $SHRINKWRAP_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo gcc
install_from_repo gcc-c++
install_from_repo wget
install_from_repo perl
install_from_repo git


# For 006 kernel compile
install_from_repo openssl-devel

# traffic shaping
# install_from_repo trickle
# TODO: uncomment once trickle is available for Centos 8
#       and enable test 056-lowspeedlimit

# Migration test needs lsb_release
echo "install lsb_release..."
install_from_repo redhat-lsb-core

install_from_repo python3
sudo ln -s /usr/bin/python3 /usr/bin/python || true

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"
