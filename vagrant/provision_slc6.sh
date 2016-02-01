#!/bin/bash

set -e

WORKSPACE="provision"
RELEASE_PKG="https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-2-5.noarch.rpm"
CVMFS_TEST_USER="sftnight"
CVMFS_SOURCE_DIR="$(pwd)/cvmfs"
VAGRANT_WORKSPACE="/vagrant"

# set VM locale
export LANG="en_US.UTF-8"
echo "LANG=\"$LANG\"" > /etc/sysconfig/i18n

# create a provisioning workspace
PREV_DIR="$(pwd)"
mkdir -p $WORKSPACE
cd $WORKSPACE

# update installed packages
yum -y --exclude='kernel*' update

# install cvmfs-release RPM
if ! rpm -q cvmfs-release > /dev/null 2>&1; then
  echo "installing cvmfs-release package"
  wget --quiet "$RELEASE_PKG"
  yum -y install $(basename $RELEASE_PKG)
fi

# jump back to the home directory
cd $PREV_DIR

# install custom kernel
if ! rpm -q kernel | grep -q 'aufs'; then
  echo "installing custom AUFS enabled kernel"
  yum -y --disablerepo='*' --enablerepo='cernvm-kernel' install kernel kernel-headers
fi

# activate EPEL to get as much of the fun stuff as possible
yum -y install epel-release

# install necessary development packages
yum -y install libuuid-devel gcc gcc-c++ glibc-common cmake fuse fuse-devel  \
               fuse-libs libattr-devel openssl openssl-devel patch pkgconfig \
               gawk perl autofs zlib gzip unzip gdb chkconfig which          \
               shadow-utils util-linux-ng selinux-policy checkpolicy         \
               selinux-policy-devel hardlink selinux-policy-targeted         \
               python-devel initscripts bash coreutils grep sed sudo psmisc  \
               curl attr httpd libcap-devel voms-devel

# install convenience packages for development
yum -y install git tig iftop htop jq rubygems screen nc python-unittest2 \
               policycoreutils-python
gem install fakes3 --version 0.2.0

# setup and run a FakeS3 server
FAKES3_PORT=13337
if [ ! -x /etc/init.d/fakes3 ]; then
  cp ${VAGRANT_WORKSPACE}/vagrant/fakes3.init.d.sh /etc/init.d/fakes3
  chmod a+x /etc/init.d/fakes3
  chkconfig --add fakes3
  chkconfig fakes3 on
  service fakes3 start
fi

# drop a FakeS3 default configuration for CVMFS server
if [ ! -f /etc/cvmfs/fakes3.default.conf ]; then
  mkdir -p /etc/cvmfs
  cp ${VAGRANT_WORKSPACE}/vagrant/fakes3.default.conf /etc/cvmfs
fi

# link the CernVM-FS source directory in place
if [ ! -L $CVMFS_SOURCE_DIR ]; then
  ln -s $VAGRANT_WORKSPACE $CVMFS_SOURCE_DIR
  chown -h vagrant:vagrant $CVMFS_SOURCE_DIR
fi

# enable httpd on boot
if ! /sbin/chkconfig httpd > /dev/null 2>&1; then
  /sbin/chkconfig --add httpd
  /sbin/chkconfig httpd on
  /sbin/service httpd start
fi

# allow user 'vagrant' to do everything
sed -i -e 's/^vagrant.*/vagrant ALL=(ALL) NOPASSWD:ALL/' /etc/sudoers

# create CVMFS test user
if ! id $CVMFS_TEST_USER > /dev/null 2>&1; then
  useradd $CVMFS_TEST_USER
  usermod -a -G fuse $CVMFS_TEST_USER
  echo "$CVMFS_TEST_USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
fi
