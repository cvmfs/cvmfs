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
keysVersion=1.4-1

curl -k https://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/5/x86_64/cvmfs-init-scripts-${initScriptsVersion}.noarch.rpm > $tmpdir/cvmfs-init-scripts-${initScriptsVersion}.noarch.rpm
curl -k https://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/5/x86_64/cvmfs-keys-${keysVersion}.noarch.rpm > $tmpdir/cvmfs-keys-${keysVersion}.noarch.rpm

sudo rpm -vi $tmpdir/cvmfs-keys-${keysVersion}.noarch.rpm $cvmfsrpms
sudo rpm -vi $tmpdir/cvmfs-init-scripts-${initScriptsVersion}.noarch.rpm
sudo cvmfs_config setup
