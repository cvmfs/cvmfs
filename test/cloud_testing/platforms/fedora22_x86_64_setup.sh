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
sudo dnf -y update || die "fail (dnf update)"

# disable SELinux (OverlayFS doesn't support it)
echo -n "set SELinux into permissive mode..."
sudo setenforce 0 || die "fail"
echo "done"

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $UNITTEST_PACKAGE

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
install_from_repo gcc
install_from_repo gcc-c++
install_from_repo rubygems
install_from_repo java

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"
