#!/bin/sh

set -e

tmpdir=$1
[ -z $tmpdir ] && exit 1
shift

cvmfsrpms=$@
for cvmfsrpm in $cvmfsrpms; do
  if [ -z $cvmfsrpm ]; then
    exit 2
  fi
done

defaultConfigVersion=1.0-1

curl -k https://ecsft.cern.ch/dist/cvmfs/cvmfs-config/cvmfs-config-default-${defaultConfigVersion}.noarch.rpm > $tmpdir/cvmfs-config-default-${defaultConfigVersion}.noarch.rpm

sudo rpm -vi $tmpdir/cvmfs-config-default-${defaultConfigVersion}.noarch.rpm $cvmfsrpms
sudo cvmfs_config setup
