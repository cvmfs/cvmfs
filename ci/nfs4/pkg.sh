#!/bin/bash

#
# Builds a macOS development package of the cvmfs client with the NFS frontend
# only (no macFUSE/FSKit libraries).  `mount -t cvmfs` uses `cvmfs2 -o nfs`.
#
# Needs the libfuse3 headers (e.g. the fuse-t fork) in /usr/local/include.
#

set -e

if [ $# -lt 2 ]; then
  echo "Usage: $0 <cvmfs source directory> <build directory> [<version suffix>]"
  exit 1
fi

SOURCE_DIR="$(cd "$1"; pwd)"
BUILD_DIR="$2"
VERSION_SUFFIX="${3:-dev}"
PREFIX=/usr/local

. ${SOURCE_DIR}/ci/common.sh

stage_dir=${BUILD_DIR}/nfs_pkg_root
result_dir=${BUILD_DIR}/pkg_result
[ ! -d $stage_dir ] || die "$stage_dir exists"
mkdir -p $stage_dir $result_dir

cvmfs_version="$(get_cvmfs_version "$SOURCE_DIR")-nfs.${VERSION_SUFFIX}"
echo "building $cvmfs_version"

export CPATH=/usr/local/include
# The vendored sqlite: cvmfs reconfigures sqlite globally, which crashes
# frameworks sharing the system libsqlite3
cmake -S $SOURCE_DIR -B $BUILD_DIR -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX=$PREFIX \
  -DBUILTIN_EXTERNALS=ON \
  -DBUILD_CVMFS=ON -DBUILD_LIBCVMFS=OFF -DBUILD_LIBCVMFS_CACHE=OFF \
  -DBUILD_SERVER=OFF -DBUILD_RECEIVER=OFF -DBUILD_GEOAPI=OFF \
  -DBUILD_UNITTESTS=OFF -DBUILD_MANPAGES=OFF \
  -DBUILD_NFS_FRONTEND=ON \
  -DBUILTIN_EXTERNALS_EXCLUDE=zlib \
  -DCMAKE_DISABLE_FIND_PACKAGE_sqlite3=TRUE
cmake --build $BUILD_DIR --target \
  cvmfs2 cvmfs_talk cvmfs_nfsfuse cvmfs_nfs_stub cvmfs_nfs cvmfs_nfs_debug \
  mount_cvmfs
if otool -L $BUILD_DIR/cvmfs/libcvmfs_nfs.dylib | grep -qi sqlite; then
  die "libcvmfs_nfs links against a shared libsqlite3"
fi

# The regular install would also need the macFUSE libraries
mkdir -p $stage_dir/bin $stage_dir/lib $stage_dir/sbin
install -m 0755 $BUILD_DIR/cvmfs/cvmfs2 $BUILD_DIR/cvmfs/cvmfs_talk \
  $SOURCE_DIR/cvmfs/cvmfs_config $stage_dir/bin/
install -m 0755 $BUILD_DIR/mount/mount_cvmfs $stage_dir/sbin/
for lib in cvmfs_nfsfuse cvmfs_nfs_stub cvmfs_nfs cvmfs_nfs_debug; do
  cp -P $BUILD_DIR/cvmfs/lib${lib}*.dylib $stage_dir/lib/
done
# Point install names and references from the build tree to $PREFIX/lib
for f in $stage_dir/bin/cvmfs2 $stage_dir/bin/cvmfs_talk \
         $stage_dir/sbin/mount_cvmfs $(find $stage_dir/lib -type f); do
  case "$f" in
    *.dylib) install_name_tool -id $PREFIX/lib/$(basename $f) $f ;;
  esac
  for dep in $(otool -L $f | awk 'NR > 1 {print $1}' | grep 'libcvmfs_'); do
    install_name_tool -change $dep $PREFIX/lib/$(basename $dep) $f
  done
  codesign --force --sign - $f
done

DESTDIR=$BUILD_DIR/nfs_pkg_destdir cmake --install $BUILD_DIR/mount
cp -R $BUILD_DIR/nfs_pkg_destdir/$PREFIX/etc $stage_dir/
install -m 0644 $SOURCE_DIR/packaging/mac/nfs/60-nfs.conf \
  $stage_dir/etc/cvmfs/default.d/

pkgbuild --root $stage_dir \
         --scripts $SOURCE_DIR/packaging/mac/nfs/scripts \
         --identifier ch.cern.cvmfs.pkg.nfs \
         --version "$cvmfs_version" \
         --install-location $PREFIX \
         $result_dir/cvmfs-${cvmfs_version}.pkg
echo "created $result_dir/cvmfs-${cvmfs_version}.pkg"
