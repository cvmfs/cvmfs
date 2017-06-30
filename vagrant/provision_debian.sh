#/bin/bash

CVMFS_TEST_USER="sftnight"
CVMFS_SOURCE_DIR="$(pwd)/cvmfs"
VAGRANT_WORKSPACE="/vagrant"

# update package manager
apt-get update

# install required packages
apt-get install -y apache2 attr autofs autotools-dev bash bc cmake coreutils   \
                   curl debhelper debianutils debootstrap fuse g++             \
                   gawk gcc gdb grep gzip initscripts insserv                  \
                   libapache2-mod-wsgi libattr1-dev libc-bin libc6-dev         \
                   libcap-dev libfuse-dev libfuse2 libssl-dev make openssl     \
                   default-jre-headless patch perl pkg-config psmisc           \
                   python-dev python-lzma sed sqlite3 sudo sysvinit-utils      \
                   unzip uuid uuid-dev yum zlib1g

# install some convenience packages
apt-get install -y git tig iftop htop jq screen python-unittest2

# link the CernVM-FS source directory in place
if [ ! -L $CVMFS_SOURCE_DIR ]; then
  ln -s $VAGRANT_WORKSPACE $CVMFS_SOURCE_DIR
  chown -h vagrant:vagrant $CVMFS_SOURCE_DIR
fi

# create CVMFS test user
if ! id $CVMFS_TEST_USER > /dev/null 2>&1; then
  useradd $CVMFS_TEST_USER
  echo "$CVMFS_TEST_USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
fi
