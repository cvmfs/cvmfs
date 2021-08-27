#!/bin/bash

export PATH=/usr/local/bin:$PATH

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_setup.sh

echo "Removing traces of previous runs"
sudo cvmfs_config killall
sudo rm -f /etc/cvmfs/default.local

echo -n "Install client package: $CLIENT_PACKAGE ... "
sudo installer -pkg "$CLIENT_PACKAGE" -target / \
    || die "fail (installing CernVM-FS client package)"
echo "done"

echo -n "Setting up CernVM-FS environment... "
sudo cvmfs_config setup || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo cvmfs_config chksetup > /dev/null || die "fail (cvmfs_config chksetup)"
echo "done"

echo -n "Installing test dependencies from Homebrew ..."
install_homebrew sqlite tree cmake jq \
    || die "fail (installing test dependencies from Homebrew)"
echo "done"

echo -n "Increasing open file limits ... "
sudo launchctl limit maxfiles 65536 65536 \
    || die "fail (increasing open file limits)"
echo "done"
