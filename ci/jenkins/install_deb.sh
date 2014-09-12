#!/bin/sh

set -e

echo "Continuous Integration DEB Installation Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

cd $CVMFS_BUILD_RESULTS

tmp_dir=/tmp

echo "Downloading Extra Packages"
curl -k "https://ecsft.cern.ch/dist/cvmfs/cvmfs-keys/cvmfs-keys_${CVMFS_KEYS_VERSION}_all.deb" > ${tmp_dir}/cvmfs-keys_${CVMFS_KEYS_VERSION}_all.deb

echo "Installing Debian Packages"
sudo dpkg --install --force-confnew ${tmp_dir}/cvmfs-keys_${CVMFS_KEYS_VERSION}_all.deb
sudo dpkg --install --force-confnew *.deb

echo "Configuring CVMFS Client"
sudo cvmfs_config setup
