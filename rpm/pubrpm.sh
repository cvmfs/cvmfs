#!/bin/sh

service ntpd stop
ntpdate ptbtime1.ptb.de

TAG=$1

rpm --verbose --addsign /usr/src/redhat/SRPMS/cvmfs-$TAG-1.src.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/x86_64/cvmfs-$TAG-1.x86_64.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/i586/cvmfs-$TAG-1.i586.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/x86_64/cvmfs-server-$TAG-1.x86_64.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/x86_64/cvmfsflt-$TAG-1.x86_64.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-$TAG-1.x86_64.rpm
rpm --verbose --addsign /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-xen-$TAG-1.x86_64.rpm
rpm --verbose --addsign /usr/src/redhat/SRPMS/cvmfs-$TAG-1.src.rpm
#rpm --verbose --addsign /usr/src/redhat/RPMS/i586/cvmfs-server-$TAG-1.i586.rpm

scp /usr/src/redhat/RPMS/x86_64/cvmfs-$TAG-1.x86_64.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/RPMS/i586/cvmfs-$TAG-1.i586.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/SRPMS/cvmfs-$TAG-1.src.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/RPMS/x86_64/cvmfs-server-$TAG-1.x86_64.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/RPMS/x86_64/cvmfsflt-$TAG-1.x86_64.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-$TAG-1.x86_64.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
scp /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-xen-$TAG-1.x86_64.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/
#scp /usr/src/redhat/RPMS/i586/cvmfs-server-$TAG-1.i586.rpm root@cvmappi02.cern.ch:/srv/www/html/repository/cernvm/

kinit cvmadmin
aklog
cp /usr/src/redhat/RPMS/x86_64/cvmfs-$TAG-1.x86_64.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
cp /usr/src/redhat/RPMS/i586/cvmfs-$TAG-1.i586.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/i386/
cp /usr/src/redhat/RPMS/x86_64/cvmfs-server-$TAG-1.x86_64.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
cp /usr/src/redhat/RPMS/x86_64/cvmfsflt-$TAG-1.x86_64.rpm  /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
cp /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-$TAG-1.x86_64.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
cp /usr/src/redhat/RPMS/x86_64/kmod-cvmfsflt-xen-$TAG-1.x86_64.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64/
#cp /usr/src/redhat/RPMS/i586/cvmfs-server-$TAG-1.i586.rpm /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/i386/
createrepo /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/x86_64
createrepo /afs/cern.ch/sw/lcg/cernvm/project/repos/yum/cvmfs/i386
