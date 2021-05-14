#!/bin/sh

#
# This script builds the debian packages of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS debian packages"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

CVMFS_CONFIG_PACKAGE="cvmfs-config-default_2.0-1_all.deb"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

# generate the release tag for either a nightly build or a release
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_version="${cvmfs_version}~0.${CVMFS_NIGHTLY_BUILD_NUMBER}git${git_hash}"
  echo "creating nightly build '$cvmfs_version'"
else
  cvmfs_version="${cvmfs_version}~1"
fi
cvmfs_version="${cvmfs_version}+$(lsb_release -si | tr [:upper:] [:lower:])"
cvmfs_version="${cvmfs_version}$(lsb_release -sr)"
echo "creating release: $cvmfs_version"

# copy the entire source tree into a working directory
echo "copying source into workspace..."
mkdir -p $CVMFS_RESULT_LOCATION
copied_source="${CVMFS_RESULT_LOCATION}/wd_src"
[ ! -d $copied_source ] || die "build directory is not empty"
mkdir -p $copied_source
cp -r ${CVMFS_SOURCE_LOCATION}/* $copied_source

# produce the debian package
echo "copy packaging meta information and get in place..."
cp -r ${CVMFS_SOURCE_LOCATION}/packaging/debian/cvmfs ${copied_source}/debian
cd $copied_source

# Configure control file
mv debian/control.in debian/control
echo "Debian release: $(lsb_release -sc)"
# On Ubuntu 18.04 Bionic, the insserv and initscripts packages are no longer
# available. They should be removed from the list of dependencies
if [ x"$(lsb_release -sc)" = x"bionic" ]; then
  sed -i -e "s/insserv, initscripts, //g" debian/control
fi
# Fuse3 is only available as of Debian 10 "buster" and Ubuntu 20.04
if [ x"$(lsb_release -sc)" = x"buster" -o x"$(lsb_release -sc)" = x"focal" ]; then
  sed -i -e "s/^Build-Depends:/Build-Depends: libfuse3-dev,/g" debian/control
  sed -i -e "s/^Recommends:/Recommends: cvmfs-fuse3,/g" debian/control
else
  cat debian/control | awk '/#FUSE3-BEGIN/{flag=1;next}/#FUSE3-END/{flag=0;next}!flag' > debian/control.tmp
  mv debian/control.tmp debian/control
fi
# The cvmfs-gateway requires a go compiler
if ! go version >/dev/null 2>&1; then
  cat debian/control | awk '/#GATEWAY-BEGIN/{flag=1;next}/#GATEWAY-END/{flag=0;next}!flag' > debian/control.tmp
  mv debian/control.tmp debian/control
fi

cpu_cores=$(get_number_of_cpu_cores)
echo "do the build (with $cpu_cores cores)..."
dch -v $cvmfs_version -M "bumped upstream version number"
# -us -uc == skip signing
DEB_BUILD_OPTIONS=parallel=$cpu_cores debuild --prepend-path=/usr/local/go/bin \
  -us -uc
cd ${CVMFS_RESULT_LOCATION}

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL"                           \
                       "$(basename $(find . -name 'cvmfs_*.deb'))"          \
                       "$(basename $(find . -name 'cvmfs-server*.deb'))"    \
                       "$(basename $(find . -name 'cvmfs-dev*.deb'))"       \
                       "$(basename $(find . -name 'cvmfs-unittests*.deb'))" \
                       "$CVMFS_CONFIG_PACKAGE"                              \
                       "$(basename $(find . -name 'cvmfs-shrinkwrap*.deb'))"\
                       ""                                                   \
                       "$(basename $(find . -name 'cvmfs-fuse3*.deb'))"     \
                       "$(basename $(find . -name 'cvmfs-gateway*.deb'))"
fi

# clean up the source tree
echo "cleaning up..."
rm -fR $copied_source
