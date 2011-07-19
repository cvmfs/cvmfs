#!/bin/sh

if [ "x$1" == "x" ]; then
  exit 1
fi
if [ "x$2" == "x" ]; then
  exit 2
fi

svn cp https://cernvm.cern.ch/project/svn/cvmfs2/trunk https://cernvm.cern.ch/project/svn/cvmfs2/tags/cvmfs2-0.2.$1 -m "$2"
rm -rf /tmp/dist
svn export . /tmp/dist
cd /tmp/dist
./bootstrap.sh
./configure --enable-sqlite3-builtin 
make dist
scp cvmfs-0.2.$1.tar.gz root@cvmappi02:/srv/www/html/repository/cernvm/
cd $OLDPWD
sed -i s/0\.2\.[0-9]*/0.2.$[$1+1]/ configure.ac
sed -i s/0\.2\.[0-9]*/0.2.$[$1+1]/ rpm/cvmfs.spec
sed -i s/0\.2\.[0-9]*/0.2.$[$1+1]/ rpm/cvmfs-server.spec
sed -i s/0\.2\.[0-9]*/0.2.$[$1+1]/ rpm/cvmfsflt.spec
