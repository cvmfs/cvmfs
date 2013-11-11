#!/bin/sh

set -e

#
# This script is not called by the CI system! It is supposed to be used for
# package creation debugging and as a blue print for CI configuration.
#

usage() {
  echo "Sample script that builds cvmfs debian packages from source"
  echo "Usage: $0 <work dir> <source tree root>"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
fi

workdir=$1
srctree=$(readlink --canonicalize $2)

if [ "$(ls -A $workdir 2>/dev/null)" != "" ]; then
  echo "$workdir must be empty"
  exit 2
fi

echo -n "creating workspace in $workdir... "
mkdir ${workdir}/tmp ${workdir}/src ${workdir}/result
echo "done"

echo -n "copying source tree to $workdir/tmp... "
cp -R $srctree/* ${workdir}/tmp
echo "done"

echo -n "initializing build environment... "
mkdir ${workdir}/src/cvmfs.orig ${workdir}/src/cvmfs
cp -R $srctree/* ${workdir}/src/cvmfs.orig
cp -R $srctree/* ${workdir}/src/cvmfs
mkdir ${workdir}/src/cvmfs/debian
cp -R ${workdir}/tmp/packaging/debian/cvmfs/* ${workdir}/src/cvmfs/debian
echo "done"

echo -n "figuring out version number... "
upstream_version="$(cat ${srctree}/CMakeLists.txt | grep '## CVMFS_VERSION' | awk '{print $3}')-0"
echo "done"

echo "building..."
cd ${workdir}/src/cvmfs
dch -v $upstream_version -M "bumped upstream version number"

cd debian
pdebuild --buildresult ${workdir}/result
