#!/bin/sh

service ntpd stop
ntpdate ptbtime1.ptb.de

TAG=$1
if [ -z $TAG ]; then
  exit
fi

TRUNK=0
if [ ! -z "$2" ]; then
  TRUNK=1
fi

export RPM_BUILD_ROOT=/tmp

rm -rf cvmfs-${TAG}
rm -f cvmfs-${TAG}.tar.gz

if [ $TRUNK -eq 0 ]; then
  wget https://cernvm.cern.ch/project/trac/downloads/cernvm/cvmfs-${TAG}.tar.gz --no-check-certificate
  cp cvmfs-${TAG}.tar.gz /usr/src/redhat/SOURCES/
else
  rm -rf /tmp/dist
  mkdir /tmp/dist
  cd /tmp/dist
  svn export https://cernvm.cern.ch/project/svn/cvmfs2/trunk
  cd trunk
  ./bootstrap.sh
  ./configure --enable-sqlite3-builtin
  make dist
  cp cvmfs-${TAG}.tar.gz /usr/src/redhat/SOURCES/
  cp cvmfs-${TAG}.tar.gz ~
  cd
fi

tar xvfz cvmfs-${TAG}.tar.gz
cp cvmfs-$TAG/rpm/cvmfs.spec rpm/
cp cvmfs-$TAG/rpm/cvmfs-server.spec rpm/
cp cvmfs-$TAG/rpm/cvmfsflt.spec rpm/

cp cvmfs-$TAG/rpm/cvmfsflt-depmod.conf /usr/src/redhat/SOURCES
cp cvmfs-$TAG/rpm/slc-kmodtool /usr/src/redhat/SOURCES
rm -rf /tmp/cvmfsflt-tarball
mkdir -p /tmp/cvmfsflt-tarball/cvmfsflt-$TAG
cp cvmfs-${TAG}/kernel/cvmfsflt/src/* /tmp/cvmfsflt-tarball/cvmfsflt-$TAG
cp cvmfs-${TAG}/kernel/redirfs/src/redirfs.h /tmp/cvmfsflt-tarball/cvmfsflt-$TAG
cd /tmp/cvmfsflt-tarball
tar cvfz cvmfsflt-${TAG}.tar.gz cvmfsflt-${TAG}
cp cvmfsflt-${TAG}.tar.gz /usr/src/redhat/SOURCES
cd

cd rpm
rpmbuild -ba cvmfs.spec
CFLAGS="-m32 -march=i586" CXXFLAGS="-m32 -march=i586" LDFLAGS="-m32 -L/usr/lib" linux32 rpmbuild --rebuild --target=i586 /usr/src/redhat/SRPMS/cvmfs-$TAG-1.src.rpm
cp ../cvmfs-${TAG}.tar.gz /usr/src/redhat/SOURCES/
rpmbuild -ba cvmfs-server.spec
#CFLAGS="-m32" CXXFLAGS="-m32" LDFLAGS="-m32 -L/usr/lib" linux32 rpmbuild --rebuild --target=i586 /usr/src/redhat/SRPMS/cvmfs-server-$TAG-1.src.rpm
rpmbuild -ba cvmfsflt.spec
