#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# configuring apt for non-interactive environment
echo -n "configure package manager for non-interactive usage... "
export DEBIAN_FRONTEND=noninteractive
echo "done"

# update package manager cache
echo -n "updating package manager cache... "
sudo apt-get update > /dev/null || die "fail (apt-get update)"
echo "done"

# install package dependency resolve program
echo -n "installing gdebi-core... "
sudo apt-get install gdebi-core > /dev/null || die "fail (install gdebi-core)"
echo "done"

# install deb packages
echo "installing DEB packages... "
install_deb $CLIENT_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment... "
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo service autofs restart > /dev/null          || die "fail (restart autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install test dependencies
echo "installing test dependencies..."
install_from_repo gcc  || die "fail (installing gcc)"
install_from_repo make || die "fail (installing make)"
