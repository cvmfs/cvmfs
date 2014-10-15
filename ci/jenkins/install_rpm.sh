#!/bin/sh

set -e

echo "Continuous Integration RPM Installation Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

cd $CVMFS_BUILD_RESULTS

tmp_dir=/tmp

echo "Downloading Extra Packages"
curl -k https://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/5/x86_64/cvmfs-init-scripts-${CVMFS_INIT_SCRIPTS_VERSION}.noarch.rpm > $tmp_dir/cvmfs-init-scripts-${CVMFS_INIT_SCRIPTS_VERSION}.noarch.rpm
curl -k https://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/5/x86_64/cvmfs-keys-${CVMFS_KEYS_VERSION}.noarch.rpm                 > $tmp_dir/cvmfs-keys-${CVMFS_KEYS_VERSION}.noarch.rpm

echo "Installing packages"
sudo rpm -vi $tmp_dir/cvmfs-keys-${CVMFS_KEYS_VERSION}.noarch.rpm
sudo rpm -vi *.rpm
sudo rpm -vi $tmp_dir/cvmfs-init-scripts-${CVMFS_INIT_SCRIPTS_VERSION}.noarch.rpm

echo "Configuring CVMFS Client"
sudo cvmfs_config setup
