#!/bin/sh

svndir=$1
[ -z $svndir ] && exit 1
distdir=$2
[ -z $distdir ] && exit 2
[ -d $distdir ] && exit 9
issnapshot=$3
[ -z $issnapshot ] && exit 3

svn export $svndir $distdir || exit 4
cd $distdir || exit 5
if [ $issnapshot -eq 1 ]; then
  revision=`svn info $svndir | grep "^Revision:" | cut -d" " -f2`
  sed -i -e "s/^AC_INIT(\[CVMFS\], \[[0-9\.]*\]/AC_INIT(\[CVMFS\], [r${revision}svn]/" configure.ac
fi

./bootstrap.sh || exit 6
./configure --enable-sqlite3-builtin || exit 7
make dist || exit 8

