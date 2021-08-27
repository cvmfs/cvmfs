#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

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
install_rpm $SHRINKWRAP_PACKAGE

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo mod_wsgi || die "fail (installing mod_wsgi)"
sudo service httpd restart || die "fail (restarting apache)"

echo "installing mod_ssl for Apache"
install_from_repo mod_ssl || die "fail (installing mod_ssl)"

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
install_from_repo tree          || die "fail (installing tree)"

# traffic shaping
install_from_repo trickle || die "fail (installing trickle)"

# install `libcvmfs` build dependencies
install_from_repo openssl-devel || die "fail (installing openssl-devel)"
install_from_repo libuuid-devel || die "fail (installing libuuid-devel)"

# install `cvmfs_preload` build dependencies
install_from_repo cmake         || die "fail (installing cmake)"
install_from_repo libattr-devel || die "fail (installing libattr-devel)"
install_from_repo python-devel  || die "fail (installing python-devel)"

install_from_repo acl

# install test dependency for 600
install_from_repo compat-expat1          || die "fail (installing compat-expat1)"
install_from_repo openssl098e            || die "fail (installing openssl098e)"
install_from_repo gridsite               || die "fail (installing gridsite)"
install_from_repo voms                   || die "fail (installing voms)"
install_from_repo globus-common          || die "fail (installing globus-common)"
install_from_repo globus-gsi-callback    || die "fail (installing globus-gsi-callback)"
install_from_repo globus-gsi-cert-utils  || die "fail (installing globus-gsi-cert-utils)"
install_from_repo globus-gsi-credential  || die "fail (installing globus-gsi-credential)"
install_from_repo globus-gsi-sysconfig   || die "fail (installing globus-gsi-sysconfig)"
# TODO(jblomer): when we get support on more platforms, we might want to get the
# helper package in a more general way than hard-coding it into the setup script
contrib_release="http://ecsft.cern.ch/dist/cvmfs/cvmfs-contrib-release/cvmfs-contrib-release-latest.noarch.rpm"
echo -n "download contrib release $contrib_release... "
wget --no-check-certificate --quiet "$contrib_release" || die "download failed"
echo "OK"
echo -n "install contrib release... "
sudo rpm -ivh $(basename $contrib_release) > /dev/null || die "install failed"
echo "OK"
install_from_repo cvmfs-x509-helper || die "fail (install cvmfs-x509-helper)"

# Install test dependency for 647
install_from_repo python-flask          || die "fail (installing python-flask)"

# install the test S3 provider
install_test_s3

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

# rebooting the system (returning 0 value)
echo "sleep 1 && reboot" > killme.sh
sudo nohup sh < killme.sh &
exit 0
