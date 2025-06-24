#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

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
install_from_repo rubygems
install_from_repo wget
install_from_repo bc
install_from_repo java-1.8.0-openjdk
install_from_repo tree
install_from_repo fuse-overlayfs
install_from_repo git
install_from_repo perl

# for building kernel
install_from_repo zlib-devel
install_from_repo libattr-devel
install_from_repo openssl-devel
install_from_repo libuuid-devel

# traffic shaping
# install_from_repo trickle
# TODO: uncomment once trickle is available for Centos 9
#       and enable test 056-lowspeedlimit

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"
