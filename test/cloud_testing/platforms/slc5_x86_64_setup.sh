#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# URLs
aufs_user_tools="http://ftp.scientificlinux.org/linux/scientific/5x/x86_64/SL/aufs-0.20090202.cvs-6.sl5.x86_64.rpm"

# create additional disk partitions to accomodate CVMFS test repos
echo -n "creating additional disk partitions... "
disk_to_partition=/dev/vda
free_disk_space=$(get_unpartitioned_space $disk_to_partition)
if [ $free_disk_space -lt 25000000000 ]; then # at least 25GB required
  die "fail (not enough unpartitioned disk space - $free_disk_space bytes)"
fi
partition_size=$(( $free_disk_space / 2 - 10240000))
create_partition $disk_to_partition $partition_size || die "fail (creating partition 1)"
create_partition $disk_to_partition $partition_size || die "fail (creating partition 2)"
echo "done"

# install AUFS kernel modules and userspace tools
echo -n "downloading aufs user space tools... "
wget --no-check-certificate --quiet $aufs_user_tools || die "fail"
echo "done"

echo "installing aufs... "
install_rpm $(basename $aufs_user_tools)                   || die "fail (installing aufs)"
yum list installed | grep "kernel-module-aufs" > /dev/null || die "fail (check installed aufs)"

echo -n "activate aufs... "
kobj=$(rpm -ql $(rpm -qa | grep kernel-module-aufs) | tail -n1)
sudo /sbin/insmod $kobj || die "fail"
echo "done"

# install RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $DEVEL_PACKAGE
install_rpm $UNITTEST_PACKAGE

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo mod_wsgi || die "fail (installing mod_wsgi)"

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
install_from_repo gcc            || die "fail (installing gcc)"
install_from_repo gcc-c++        || die "fail (installing gcc-c++)"
install_from_repo python-sqlite2 || die "fail (installing python-sqlite2)"
install_from_repo java           || die "fail (installing java)"

# traffic shaping
install_from_repo trickle || die "fail (installing trickle)"

# install `libcvmfs` build dependencies
install_from_repo openssl-devel  || die "fail (installing openssl-devel)"

# install `cvmfs_preload` build dependencies
install_from_repo cmake          || die "fail (installing cmake)"
install_from_repo libattr-devel  || die "fail (installing libattr-devel)"

# install test dependency for 600
install_from_repo gridsite || die "fail (installing gridsite)"

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

# rebooting the system (returning 0 value)
echo "sleep 1 && reboot" > killme.sh
sudo nohup sh < killme.sh &
exit 0
