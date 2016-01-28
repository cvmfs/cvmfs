#!/bin/bash

set -e

CVMFS_TEST_USER="sftnight"
CVMFS_SOURCE_DIR="$(pwd)/cvmfs"
VAGRANT_WORKSPACE="/vagrant"

# set VM locale
export LANG="en_US.UTF-8"
echo "LANG=\"$LANG\"" > /etc/sysconfig/i18n

# update packages
dnf -y update

# install necessary development packages
dnf -y install libuuid-devel gcc gcc-c++ glibc-common cmake fuse fuse-devel  \
               fuse-libs libattr-devel openssl openssl-devel patch pkgconfig \
               gawk perl autofs zlib gzip unzip gdb chkconfig which          \
               shadow-utils util-linux-ng selinux-policy checkpolicy         \
               selinux-policy-devel hardlink selinux-policy-targeted         \
               python-devel initscripts bash coreutils grep sed sudo psmisc  \
               curl attr httpd libcap-devel mod_wsgi rpm-build java wget     \
               voms-devel

# install convenience packages for development
dnf -y install git tig iftop htop jq rubygems rubygem-bundler ruby-devel \
               screen nc python-unittest2 strace lsof vim
gem install fakes3

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
if ! systemctl status httpd > /dev/null 2>&1; then
  systemctl enable httpd > /dev/null 2>&1
  systemctl start  httpd > /dev/null 2>&1
fi

# load overlayfs kernel module on boot
if ! cat /proc/filesystems | grep -q overlay; then
  modprobe overlay
  module_boot_script="/etc/sysconfig/modules/cvmfs.modules"
  cat > $module_boot_script << EOF
#!/bin/sh
exec /sbin/modprobe overlay > /dev/null 2>&1
EOF
  chmod +x $module_boot_script
fi

# allow user 'vagrant' to do everything
echo "vagrant ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers

# create CVMFS test user
if ! id $CVMFS_TEST_USER > /dev/null 2>&1; then
  useradd $CVMFS_TEST_USER
  echo "$CVMFS_TEST_USER ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers
fi

echo "all done"
