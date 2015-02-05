#!/bin/sh -e

set -e

echo "Continuous Integration Build Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# figure out some information about the version to be built
archive_dir="$(pwd)/source_archive"

# files and directories we want to have in the archive
tarball_git_files="AUTHORS CMakeLists.txt COPYING CPackLists.txt  ChangeLog INSTALL NEWS README InstallerResources add-ons bootstrap.sh cmake config_cmake.h.in cvmfs doc externals keys mount test"
tarball_file_prefix="$CVMFS_BUILD_TAG/"
tarball=${archive_dir}/${CVMFS_BUILD_TAG}.tar.gz

echo -n "Creating Source Tarball... "
mkdir -p $archive_dir
git archive --prefix=$tarball_file_prefix \
            --format=tar                  \
            $CVMFS_COMMIT                 \
            $tarball_git_files | gzip -c > $tarball
echo "done"

echo "Building Packages"
case $PACKAGE_TYPE in
  rpm)
    sh ci/jenkins/build_rpm.sh "$tarball"
    ;;
  deb)
    sh ci/jenkins/build_deb.sh "$tarball"
    ;;
  *)
    echo "unknown package type '$PACKAGE_TYPE'"
    exit 1
esac
