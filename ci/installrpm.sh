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

initScriptsVersion=1.0.18-2
configVersion=1.0-1

curl -k https://ecsft.cern.ch/dist/cvmfs/cvmfs-config/cvmfs-config-cern-${configVersion}.noarch.rpm > $tmpdir/cvmfs-config-${configVersion}.noarch.rpm

sudo rpm -vi $tmpdir/cvmfs-config-${configVersion}.noarch.rpm $cvmfsrpms
sudo cvmfs_config setup
