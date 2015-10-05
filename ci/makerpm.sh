#!/bin/sh

rpmdir=$1
[ -z $rpmdir ] && exit 1
tarball=$2
[ -z $tarball ] && exit 2
packagedir=$3
[ -z $packagedir ] && exit 3
prerelease=$4
[ -z $prerelease ] && exit 9

`dirname $0`/cleanup_node.sh

mkdir $packagedir/{BUILD,RPMS,SOURCES,SRPMS,TMP}

cp ${rpmdir}/cvmfs-universal.spec $packagedir || exit 4
cp ${rpmdir}/cvmfs.te ${rpmdir}/cvmfs.fc $packagedir/SOURCES || exit 8
cp $tarball $packagedir/SOURCES || exit 5
cd $packagedir || exit 6
version=`basename $tarball | sed 's/^cvmfs-//' | sed 's/\.tar\.gz//'`
echo $version | grep 'git'
if [ $? -eq 0 ]; then
  version_num=`echo $version | sed 's/^git-//g'`
  sed -i -e "s/^Release: .*/Release: 0.$prerelease.${version_num}git%{?dist}/" cvmfs-universal.spec || exit 7
  sed -i -e "s/\(^Source0: .*\)%{version}/\1$version/" cvmfs-universal.spec || exit 7
  sed -i -e "s/^%setup -q/%setup -q -n cvmfs-$version/" cvmfs-universal.spec || exit 7
fi

rpmbuild --define="_topdir $packagedir" --define="_tmppath ${packagedir}/TMP" -ba cvmfs-universal.spec

