#!/bin/sh
set -e

NEW_MINOR=$1
NEW_PATCH=$2
NEW_PRERELEASE=${3:-}
[ -n "$NEW_MINOR" ] || exit 1
[ -n "$NEW_PATCH" ] || exit 1

VERSION=$(sed -n '1p' VERSION)
NEW_VERSION="$(echo "$VERSION" | cut -d. -f1).${NEW_MINOR}.${NEW_PATCH}"

echo "Current base version: $VERSION"
echo "New base version: $NEW_VERSION"
if [ -n "$NEW_PRERELEASE" ]; then
  echo "Note: prerelease versions now come from an exact Git tag or" \
       "CVMFS_VERSION_OVERRIDE; '$NEW_PRERELEASE' is not stored."
fi

echo "Patching libcvmfs"
sed -i -e \
  "s/^#define LIBCVMFS_VERSION_MINOR \(.*\)/#define LIBCVMFS_VERSION_MINOR $NEW_MINOR/" \
  cvmfs/libcvmfs.h
grep VERSION cvmfs/libcvmfs.h

echo "Patching VERSION"
printf '%s\n' "$NEW_VERSION" > VERSION
cat VERSION
