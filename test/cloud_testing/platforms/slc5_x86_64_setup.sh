#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# URLs
aufs_user_tools="http://ftp.scientificlinux.org/linux/scientific/5x/x86_64/SL/aufs-0.20090202.cvs-6.sl5.x86_64.rpm"

# install AUFS kernel modules and userspace tools
echo -n "downloading aufs user space tools... "
wget --no-check-certificate --quiet $aufs_user_tools || die "fail"
echo "done"

echo "installing aufs... "
install_rpm $(basename $aufs_user_tools)                   || die "fail (installing aufs)"
yum list installed | grep "kernel-module-aufs" > /dev/null || die "fail (check installed aufs)"
echo "done"

echo -n "activate aufs... "
kobj=$(rpm -ql $(rpm -qa | grep kernel-module-aufs) | tail -n1)
sudo /sbin/insmod $kobj || die "fail"
echo "done"

# install RPM packages
echo "installing RPM packages... "
install_rpm $KEYS_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $UNITTEST_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment... "
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# start apache
echo -n "starting apache... "
sudo /sbin/service httpd start > /dev/null 2>&1 || die "fail"
echo "OK"

# install test dependencies
echo "installing test dependencies..."
install_from_repo gcc     || die "fail (installing gcc)"
install_from_repo gcc-c++ || die "fail (installing gcc-c++)"
