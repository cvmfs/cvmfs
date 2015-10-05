#!/bin/sh

gitdir=$1
[ -z $gitdir ] && exit 1
distdir=$2
[ -z $distdir ] && exit 2
issnapshot=$3
[ -z $issnapshot ] && exit 3

mkdir -p $distdir
cd $gitdir
treeish=`git show master | head -n1 | cut -d" " -f2 | head -c16`
if [ x"$issnapshot" = x"true" ] || [ x"$issnapshot" = x"1" ]; then
  version="git-$treeish"  
else
  version=`grep CVMFS_VERSION CMakeLists.txt | cut -d" " -f3`
fi
echo "in directory `pwd`"
echo "taring cvmfs version $version"

git archive -v --prefix cvmfs-${version}/ --format tar master AUTHORS CMakeLists.txt COPYING CPackLists.txt  ChangeLog INSTALL NEWS README InstallerResources add-ons bootstrap.sh cmake config_cmake.h.in cvmfs doc externals keys mount test | gzip -c > $distdir/cvmfs-${version}.tar.gz || exit 8
ls $distdir

