#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# CernVM-FS server needs 'jq' from epel
echo "enabling epel yum repository..."
install_from_repo epel-release

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $LIBS_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $DEVEL_PACKAGE
install_rpm $UNITTEST_PACKAGE
install_rpm $SHRINKWRAP_PACKAGE
install_rpm $GATEWAY_PACKAGE

# installing WSGI apache module
echo "installing python WSGI module..."
install_from_repo python3-mod_wsgi || die "fail (installing mod_wsgi)"
sudo systemctl restart httpd       || die "fail (restarting apache)"

echo "installing mod_ssl for Apache"
install_from_repo mod_ssl || die "fail (installing mod_ssl)"

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo gcc
install_from_repo gcc-c++
install_from_repo rubygems
install_from_repo wget
install_from_repo bc
install_from_repo java-1.8.0-openjdk
#install_from_repo redhat-lsb-core
install_from_repo tree
install_from_repo fuse-overlayfs

# traffic shaping
# install_from_repo trickle
# TODO: uncomment once trickle is available for Centos 9
#       and enable test 056-lowspeedlimit

# Install test dependency for 647
install_from_repo python3-pip
sudo pip3 install flask                      || die "fail (installing flask)"

# Install test dependency for 604
install_from_repo python3
install_from_repo netcat

# Install test dependency for 598
install_from_repo patch

# Install the test S3 provider
install_test_s3

# Install azurite
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
echo -e "[azure-cli]
name=Azure CLI
baseurl=https://packages.microsoft.com/yumrepos/azure-cli
enabled=1
gpgcheck=1
gpgkey=https://packages.microsoft.com/keys/microsoft.asc" | sudo tee /etc/yum.repos.d/azure-cli.repo
sudo yum install -y azure-cli
sudo yum install -y nodejs npm
sudo npm install -g azurite

# building kernel
install_from_repo perl

# building preloader
install_from_repo cmake
install_from_repo zlib-devel
install_from_repo libattr-devel
install_from_repo openssl-devel
install_from_repo libuuid-devel
install_from_repo python3-devel
install_from_repo unzip
install_from_repo bzip2
install_from_repo acl
install_from_repo git

sudo ln -s /usr/bin/python3 /usr/bin/python || true


# Migration test needs lsb_release
# echo "install lsb_release..."
# install_from_repo redhat-lsb-core

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"

disable_systemd_rate_limit

# Allow for proxying pass-through repositories
sudo setsebool -P httpd_can_network_connect on

# Ensure Apache is up and running after package update
sudo systemctl restart httpd || die "failure in final Apache restart"
