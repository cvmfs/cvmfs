#!/bin/sh -e

set -e

echo "Continuous Integration RPM Build Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

if [ $# -ne 1 ]; then
  echo "USAGE: $0 <source tarball>"
  exit 1
fi

# collect information about the package to be built
source_tarball="$1"
git_dir="$(pwd)"
build_dir="$(pwd)/${CVMFS_BUILD_DIR}"
results_dir="$(pwd)/${CVMFS_BUILD_RESULTS}"
prerelease=0 # TODO: needs some sense!

# create build environments
mkdir -p ${build_dir}/BUILD
mkdir -p ${build_dir}/RPMS
mkdir -p ${build_dir}/SOURCES
mkdir -p ${build_dir}/SRPMS
mkdir -p ${build_dir}/TMP
mkdir -p $results_dir

# prepare build environment
cd $build_dir
cp ${git_dir}/packaging/rpm/cvmfs-universal.spec .
cp ${git_dir}/packaging/rpm/cvmfs.te             ./SOURCES
cp $source_tarball                               ./SOURCES

# configure spec file (if necessary)
case $BUILD_TYPE in
  nightly|incremental)
    version_num="$(echo $CVMFS_SNAPSHOT_VERSION | sed 's/^git-//g')"
    sed -i -e "s/^Release: .*/Release: 0.${prerelease}.${version_num}git%{?dist}/" cvmfs-universal.spec
    sed -i -e "s/\(^Source0: .*\)%{version}/\1$CVMFS_SNAPSHOT_VERSION/"                    cvmfs-universal.spec
    sed -i -e "s/^%setup -q/%setup -q -n cvmfs-$CVMFS_SNAPSHOT_VERSION/"                   cvmfs-universal.spec
    ;;
  release)
    echo "Building a release!"
    ;;
  *)
    echo "FAIL: unknown build type '$BUILD_TYPE'"
    echo 1
    ;;
esac

# work around for compiler architecture flags
enforce_target=
if grep -q "6\." /etc/redhat-release 2>/dev/null; then
  if [ "$(uname -m)" = "i686" ]; then
    enforce_target="--target=i686"
  fi
fi
if [ "$(uname -m)" = "x86_64" -a $(getconf LONG_BIT) = 32 ]; then
  enforce_target="--target=i686"
fi

# build the package
rpmbuild --define="_topdir $build_dir" --define="_tmppath ${build_dir}/TMP" $enforce_target -ba cvmfs-universal.spec

# copy the built RPMs in a central location
cp ./RPMS/*/*.rpm $results_dir
