#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# create additional disk partitions to accomodate CVMFS cache
echo -n "creating additional disk partitions... "
disk_to_partition=/dev/vda
free_disk_space=$(get_unpartitioned_space $disk_to_partition)
if [ $free_disk_space -lt 10789847040 ]; then # at least 10GiB required
  die "fail (not enough unpartitioned disk space - $free_disk_space bytes)"
fi
cache_partition_size=10737418240 # 10 GiB
create_partition $disk_to_partition $cache_partition_size  || die "fail (creating partition 1)"
echo "done"

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE   # only needed for tbb shared libs (unit tests)
install_rpm $UNITTEST_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo sh -c "echo CVMFS_SEND_INFO_HEADER=yes > /etc/cvmfs/site.conf"
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

