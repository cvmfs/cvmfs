#!/bin/sh

service ntpd stop
ntpdate ptbtime1.ptb.de

TAG=$1
export RPM_BUILD_ROOT=/tmp

cd /tmp
svn export https://cernvm.cern.ch/project/svn/cvmfs-init-scripts/tags/cvmfs-init-scripts-$TAG
svn export https://cernvm.cern.ch/project/svn/cvmfs2/trunk
tar cvfz cvmfs-init-scripts-${TAG}.tar.gz cvmfs-init-scripts-$TAG
mv cvmfs-init-scripts-${TAG}.tar.gz /usr/src/redhat/SOURCES/
cp trunk/rpm/cvmfs-init-scripts.spec /root/rpm
cd /root/rpm
rpmbuild -ba --target=noarch cvmfs-init-scripts.spec
rm -rf /tmp/cvmfs-init-scripts-$TAG
rm -rf /tmp/trunk

