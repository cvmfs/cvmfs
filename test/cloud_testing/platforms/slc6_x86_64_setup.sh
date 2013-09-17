#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# custom kernel packages
knl_firmware="http://ecsft.cern.ch/dist/cvmfs/kernel/2.6.32-358.18.1.el6/kernel-firmware-2.6.32-358.18.1.el6.aufs21.x86_64.rpm"
knl="http://ecsft.cern.ch/dist/cvmfs/kernel/2.6.32-358.18.1.el6/kernel-2.6.32-358.18.1.el6.aufs21.x86_64.rpm"
aufs_util="http://ecsft.cern.ch/dist/cvmfs/kernel/aufs2-util/aufs2-util-2.1-2.x86_64.rpm"

# download the custom kernel RPMs (including AUFS)
echo -n "download custom kernel RPMs... "
wget $knl_firmware > /dev/null 2>&1 || die "fail"
wget $knl          > /dev/null 2>&1 || die "fail"
echo "done"

# install custom kernel
echo -n "install custom kernel RPMs... "
sudo rpm -ivh $(basename $knl_firmware) > /dev/null 2>&1 || die "fail"
sudo rpm -ivh $(basename $knl)          > /dev/null 2>&1 || die "fail"
echo "done"

# download AUFS user space tools
echo -n "download AUFS user space utilities... "
wget $aufs_util > /dev/null 2>&1 || die "fail"
echo "done"

# install AUFS user space utilities
echo "install AUFS utilities... "
install_rpm $(basename $aufs_util)

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm $KEYS_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $UNITTEST_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo gcc
install_from_repo gcc-c++

# rebooting the system (returning 0 value)
echo "sleep 1 && reboot" > killme.sh
sudo nohup sh < killme.sh &
exit 0
