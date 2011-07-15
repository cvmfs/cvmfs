#!/bin/sh

TAG=$1
if [ "x$TAG" == "x" ]; then
  exit 1
fi

rpm --verbose --addsign /usr/src/redhat/RPMS/noarch/$TAG-1.noarch.rpm

scp /usr/src/redhat/RPMS/noarch/$TAG-1.noarch.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/SRPMS/$TAG-1.src.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/

kinit cvmadmin
aklog
cp /usr/src/redhat/RPMS/noarch/$TAG-1.noarch.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
cp /usr/src/redhat/RPMS/noarch/$TAG-1.noarch.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/i386/
createrepo /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64
createrepo /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/i386
