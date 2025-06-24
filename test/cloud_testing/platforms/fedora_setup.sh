#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# update packages installed on the system
echo "updating installed RPM packages..."
sudo dnf -y update || echo "---- WARNING: dnf reported non-zero status code"

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $LIBS_PACKAGE
install_rpm $FUSE3_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $DEVEL_PACKAGE
install_rpm $UNITTEST_PACKAGE
install_rpm $SHRINKWRAP_PACKAGE

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo mod_wsgi || die "fail (installing mod_wsgi)"
sudo systemctl start httpd || die "fail (starting apache)"

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo file
install_from_repo gcc
install_from_repo gcc-c++
install_from_repo rubygems
install_from_repo java
install_from_repo python
install_from_repo nc
install_from_repo wget
install_from_repo bc
install_from_repo tree
install_from_repo sqlite
install_from_repo bzip2
install_from_repo fuse-overlayfs


# install build dependencies for `libcvmfs`
install_from_repo openssl-devel
install_from_repo libuuid-devel

# install stuff necessary to build `cvmfs_preload`
install_from_repo make
install_from_repo cmake
install_from_repo patch
install_from_repo zlib-devel
install_from_repo libattr-devel
install_from_repo python-devel
install_from_repo unzip
install_from_repo redhat-rpm-config

install_from_repo acl

disable_systemd_rate_limit

# Allow for proxying pass-through repositories
sudo setsebool -P httpd_can_network_connect on

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

# Enable user namespaces
sudo sysctl -w user.max_user_namespaces=10000 || die "fail (enable user namespace)"
