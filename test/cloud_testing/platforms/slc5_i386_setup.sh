#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# install RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE   # only needed for tbb shared libs (unit tests)
install_rpm $UNITTEST_PACKAGE

# we need to disable SELinux for the x86 version of SLC5
echo -n "disabling SELinux enforcing for SLC5 x86... "
echo 0 | sudo tee /selinux/enforce || die "fail"
echo "done"

# setup environment
echo -n "setting up CernVM-FS environment... "
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install test dependencies
echo "installing test dependencies..."
install_from_repo gcc
