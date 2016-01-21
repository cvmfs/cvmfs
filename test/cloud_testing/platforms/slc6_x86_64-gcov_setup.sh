#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

CURRENT_DIRECTORY="$(pwd)"
SOURCE_DIRECTORY=""
LOG_DIRECTORY=""

# parse script parameters (same for all platforms)
while getopts "t:l:" option; do
  case $option in
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done


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
wget --no-check-certificate $knl_firmware || die "fail"
wget --no-check-certificate $knl          || die "fail"
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

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo mod_wsgi || die "fail (installing mod_wsgi)"
sudo service httpd restart || die "fail (restarting apache)"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo gcc
install_from_repo gcc-c++
install_from_repo rubygems
install_from_repo java
install_from_repo libuuid-devel
install_from_repo cmake
install_from_repo fuse-devel
install_from_repo libattr-devel
install_from_repo openssl-devel
install_from_repo patch
install_from_repo pkgconfig
install_from_repo python-devel
install_from_repo unzip
install_from_repo gawk
install_from_repo perl
install_from_repo psmisc
install_from_repo autofs
install_from_repo fuse
install_from_repo attr
install_from_repo zlib
install_from_repo gdb
install_from_repo chkconfig
install_from_repo fuse-libs
install_from_repo glibc-common
install_from_repo shadow-utils
install_from_repo sysvinit-tools
install_from_repo util-linux-ng
install_from_repo checkpolicy
install_from_repo selinux-policy-devel
install_from_repo hardlink
install_from_repo selinux-policy-targeted
install_from_repo openssl
install_from_repo initscripts
install_from_repo gzip

# traffic shaping
install_from_repo trickle

# install ruby gem for FakeS3
install_ruby_gem fakes3

# build and install CernVM-FS from source
cd $SOURCE_DIRECTORY
mkdir -p build && cd build
cmake -DBUILD_SERVER=yes -DBUILD_UNITTESTS=no -DBUILD_COVERAGE=yes ..
make -j $(get_number_of_cpu_cores)
sudo make install
cd $CURRENT_DIRECTORY

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

# rebooting the system (returning 0 value)
echo "sleep 1 && reboot" > killme.sh
sudo nohup sh < killme.sh &
exit 0
