#!/bin/sh

#
# This script assumes a vanilla SLC5 (as provided by the agile cloud infra-
# structure) and installs all dependencies needed for a proper CernVM-FS test
# run.
#

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# install dependencies
sudo yum -y install autofs gdb fuse fuse-devel fuse-libs gcc gcc-c++ openssl-devel libattr-devel httpd

# install perl environment
prev_dir=$(pwd)
mkdir perl
cd perl

wget http://download.zeromq.org/zeromq-2.2.0.tar.gz || die "Failed to download ZeroMQ sources"
tar -xzf zeromq-2.2.0.tar.gz
cd zeromq-2.2.0
./configure --prefix=/usr --libdir=/usr/lib64       || die "Failed to configure ZeroMQ"
make                                                || die "Failed to compile ZeroMQ"
sudo make install                                   || die "Failed to install ZeroMQ"

sudo curl -o /usr/bin/cpanm -L http://cpanmin.us    || die "Failed to download cpanm"
sudo chmod 555 /usr/bin/cpanm

sudo cpanm ZeroMQ        --prompt                   || die "Failed to install ZeroMQ perl bindings"
sudo cpanm IO::Interface --prompt                   || die "Failed to install IO::Interface"
sudo cpanm Socket        --prompt                   || die "Failed to install Socket"
sudo cpanm URI           --prompt                   || die "Failed to install URI"

cd $prev_dir

# install RPM packages
echo "installing RPM packages... "
install_rpm "CernVM-FS keys"   $KEYS_PACKAGE
install_rpm "CernVM-FS client" $CLIENT_PACKAGE
install_rpm "CernVM-FS server" $SERVER_PACKAGE

# setup environment
echo "setting up CernVM-FS environment..."
sudo cvmfs_config setup
sudo cvmfs_config chksetup
sudo /usr/sbin httpd

sudo mkdir -p /var/log/cvmfs-test
sudo chown sftnight:sftnight /var/log/cvmfs-test

# run tests
echo ""
echo ""
echo ""
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/5*
