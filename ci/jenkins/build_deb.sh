#!/bin/sh -e

set -e

echo "Continuous Integration DEB Build Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

if [ $# -ne 1 ]; then
  echo "USAGE: $0 <source tarball>"
  exit 1
fi

# collect information about the package to be built
source_tarball="$1"

upstream_version="???"
case $BUILD_TYPE in
  nightly|incremental)
    upstream_version="${CVMFS_VERSION}.${BUILD_NUMBER}${CVMFS_SNAPSHOT_VERSION}"
    ;;
  release)
    upstream_version="$CVMFS_VERSION"
    ;;
  *)
    echo "FAIL: unknown build type '$BUILD_TYPE'"
    echo 1
    ;;
esac

# create build environment
git_dir="$(pwd)"
build_dir="$(pwd)/${CVMFS_BUILD_DIR}"
results_dir="$(pwd)/${CVMFS_BUILD_RESULTS}"
mkdir -p $build_dir $results_dir

# extract source tarball (for original source tree)
cd $build_dir
cp $source_tarball .
tar xzf $(basename $source_tarball)
mv ${CVMFS_BUILD_TAG} cvmfs_${upstream_version}.orig

# extract source tarball (for build tree)
tar xzf $(basename $source_tarball)
cd ${CVMFS_BUILD_TAG}

# prepare the source tarball for packaging
cp -Rv $git_dir/packaging/debian/cvmfs ./debian
dch -v $upstream_version -M "bumped upstream version number"
cd debian

# build the packages
pdebuild --buildresult $results_dir
