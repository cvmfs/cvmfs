#!/bin/sh
set -e

NEW_MINOR=$1
NEW_PATCH=$2
NEW_PRERELEASE=$3
[ -z "$NEW_MINOR" ] && exit 1
[ -z "$NEW_PATCH" ] && exit 1

VERSION=$(grep "^## CVMFS_VERSION" CMakeLists.txt | cut -d" " -f3)
PRERELEASE=$(grep "^## CVMFS_PRERELEASE" CMakeLists.txt | cut -d" " -f3)
echo "Current version: $VERSION$PRERLEASE"

VERSION="$(echo $VERSION | cut -d. -f1).${NEW_MINOR}.${NEW_PATCH}"
PRERELEASE=${NEW_PRERELEASE}
echo "New version: $VERSION$PRERELEASE"

echo "Patching libcvmfs"
sed -i -e "s/^#define LIBCVMFS_VERSION_MINOR \(.*\)/#define LIBCVMFS_VERSION_MINOR $NEW_MINOR/" cvmfs/libcvmfs.h
grep VERSION cvmfs/libcvmfs.h

echo "Patching CMakeLists.txt"
sed -i -e "s/^## CVMFS_VERSION \(.*\)/## CVMFS_VERSION $VERSION/" CMakeLists.txt
sed -i -e "s/^## CVMFS_PRERELEASE \(.*\)/## CVMFS_PRERELEASE $PRERELEASE/" CMakeLists.txt
sed -i -e "s/^set (CernVM-FS_VERSION_MINOR \(.*\)/set (CernVM-FS_VERSION_MINOR $NEW_MINOR)/" CMakeLists.txt
sed -i -e "s/^set (CernVM-FS_VERSION_PATCH \(.*\)/set (CernVM-FS_VERSION_PATCH $NEW_PATCH)/" CMakeLists.txt
grep VERSION CMakeLists.txt

echo "Patching RPM"
sed -i -e "s/^Version: \(.*\)/Version: $VERSION$PRERELEASE/" packaging/rpm/cvmfs-universal.spec
grep Version packaging/rpm/cvmfs-universal.spec
