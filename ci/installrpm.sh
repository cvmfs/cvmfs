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

cernConfigVersion=1.0-1
egiConfigVersion=1.0-1

curl -k https://ecsft.cern.ch/dist/cvmfs/cvmfs-config/cvmfs-config-cern-${cernConfigVersion}.noarch.rpm > $tmpdir/cvmfs-config-cern-${cernConfigVersion}.noarch.rpm
curl -k https://ecsft.cern.ch/dist/cvmfs/cvmfs-config/cvmfs-config-egi-${egiConfigVersion}.noarch.rpm > $tmpdir/cvmfs-config-egi-${egiConfigVersion}.noarch.rpm

sudo rpm -vi $tmpdir/cvmfs-config-cern-${cernConfigVersion}.noarch.rpm $tmpdir/cvmfs-config-egi-${egiConfigVersion}.noarch.rpm $cvmfsrpms
sudo cvmfs_config setup
