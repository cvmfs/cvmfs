#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

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

# update packages installed on the system
echo "updating installed RPM packages (including kernel)..."
sudo yum -y update || die "fail (yum update)"

# custom kernel packages (figures out the newest installed kernel, downloads and
#                         installs the associated patched aufs version of it)
knl_version=$(rpm -qa --last | grep -e '^kernel-[0-9]' | head -n 1 | sed -e 's/^kernel-\(.*\)\.x86_64.*$/\1/')
aufs_util_version="2.1-2"
knl_firmware="https://ecsft.cern.ch/dist/cvmfs/kernel/${knl_version}/kernel-firmware-${knl_version}.aufs21.x86_64.rpm"
knl="https://ecsft.cern.ch/dist/cvmfs/kernel/${knl_version}/kernel-${knl_version}.aufs21.x86_64.rpm"
aufs_util="https://ecsft.cern.ch/dist/cvmfs/kernel/aufs2-util/aufs2-util-${aufs_util_version}.x86_64.rpm"

# download the custom kernel RPMs (including AUFS)
echo -n "download custom kernel RPMs for $knl_version ... "
wget --no-check-certificate --quiet $knl_firmware || die "fail"
wget --no-check-certificate --quiet $knl          || die "fail"
echo "done"

# install custom kernel
echo -n "install custom kernel RPMs... "
sudo rpm -ivh $(basename $knl_firmware) > /dev/null || die "fail"
sudo rpm -ivh $(basename $knl)          > /dev/null || die "fail"
echo "done"

# download AUFS user space tools
echo -n "download AUFS user space utilities... "
wget --no-check-certificate --quiet $aufs_util || die "fail"
echo "done"

# install AUFS user space utilities
echo "install AUFS utilities... "
install_rpm $(basename $aufs_util)

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $DEVEL_PACKAGE
install_rpm $UNITTEST_PACKAGE

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo mod_wsgi || die "fail (installing mod_wsgi)"
sudo service httpd restart || die "fail (restarting apache)"

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
install_from_repo gcc           || die "fail (installing gcc)"
install_from_repo gcc-c++       || die "fail (installing gcc-c++)"
install_from_repo rubygems      || die "fail (installing rubygems)"
install_from_repo java          || die "fail (installing java)"

# traffic shaping
install_from_repo trickle || die "fail (installing trickle)"

# install `libcvmfs` build dependencies
install_from_repo openssl-devel || die "fail (installing openssl-devel)"
install_from_repo libuuid-devel || die "fail (installing libuuid-devel)"

# install `cvmfs_preload` build dependencies
install_from_repo cmake         || die "fail (installing cmake)"
install_from_repo libattr-devel || die "fail (installing libattr-devel)"

# install test dependency for 600
install_from_repo compat-expat1 || die "fail (installing compat-expat1)"
install_from_repo openssl098e   || die "fail (installing openssl098e)"
install_from_repo gridsite      || die "fail (installing gridsite)"
install_from_repo voms          || die "fail (installing voms)"

# install ruby gem for FakeS3
install_ruby_gem fakes3 0.2.0  # latest is 0.2.1 (23.07.2015) that didn't work.

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

# rebooting the system (returning 0 value)
echo "sleep 1 && reboot" > killme.sh
sudo nohup sh < killme.sh &
exit 0
