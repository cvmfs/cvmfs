#!/bin/bash

#
# Builds a macOS development package of the cvmfs client with the NFS frontend
# only (no macFUSE/FSKit libraries).  `mount -t cvmfs` uses `cvmfs2 -o nfs`.
# Needs no FUSE installation.
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

# The vendored sqlite: cvmfs reconfigures sqlite globally, which crashes
# frameworks sharing the system libsqlite3
cmake -S $SOURCE_DIR -B $BUILD_DIR -G Ninja \
  -DCMAKE_INSTALL_PREFIX=$PREFIX \
  -DBUILTIN_EXTERNALS=ON \
  -DBUILD_CVMFS=ON -DBUILD_LIBCVMFS=OFF -DBUILD_LIBCVMFS_CACHE=OFF \
  -DBUILD_SERVER=OFF -DBUILD_RECEIVER=OFF -DBUILD_GEOAPI=OFF \
  -DBUILD_UNITTESTS=OFF -DBUILD_MANPAGES=OFF \
  -DCVMFS_MACFUSE=OFF -DBUILD_NFS_FRONTEND=ON \
  -DBUILTIN_EXTERNALS_EXCLUDE=zlib \
  -DCMAKE_DISABLE_FIND_PACKAGE_sqlite3=TRUE
cmake --build $BUILD_DIR --target \
  cvmfs2 cvmfs_fsck cvmfs_talk cvmfs_nfsfuse cvmfs_nfs_stub cvmfs_nfs cvmfs_nfs_debug \
  mount_cvmfs
if otool -L $BUILD_DIR/cvmfs/libcvmfs_nfs.dylib | grep -qi sqlite; then
  die "libcvmfs_nfs links against a shared libsqlite3"
fi

# The regular install would also need the macFUSE libraries
mkdir -p $stage_dir/bin $stage_dir/lib $stage_dir/sbin
install -m 0755 $BUILD_DIR/cvmfs/cvmfs2 $BUILD_DIR/cvmfs/cvmfs_fsck \
  $BUILD_DIR/cvmfs/cvmfs_talk \
  $SOURCE_DIR/cvmfs/cvmfs_config $stage_dir/bin/
install -m 0755 $BUILD_DIR/mount/mount_cvmfs $stage_dir/sbin/
for lib in cvmfs_nfsfuse cvmfs_nfs_stub cvmfs_nfs cvmfs_nfs_debug; do
  cp -P $BUILD_DIR/cvmfs/lib${lib}*.dylib $stage_dir/lib/
done
# Bundle every non-system dependency: cvmfs libraries into $PREFIX/lib,
# third-party ones (e.g. Homebrew openssl, c-ares) into $PREFIX/lib/cvmfs so
# that they do not clash with a Homebrew installation in /usr/local
bundle_dir=$stage_dir/lib/cvmfs
mkdir -p $bundle_dir
target_path() {
  case "$(basename $1)" in
    libcvmfs_*) echo $PREFIX/lib/$(basename $1) ;;
    *)          echo $PREFIX/lib/cvmfs/$(basename $1) ;;
  esac
}
resolve() {
  case "$1" in
    @rpath/*) for d in $BUILD_DIR/cvmfs /opt/homebrew/lib /usr/local/lib; do
                [ -e $d/${1#@rpath/} ] && { echo $d/${1#@rpath/}; return; }
              done ;;
    $PREFIX/lib/libcvmfs_*)
      find $BUILD_DIR -name $(basename $1) -not -path "$stage_dir/*" | head -1 ;;
    *) echo $1 ;;
  esac
}
changed=1
while [ $changed -eq 1 ]; do
  changed=0
  for f in $stage_dir/bin/cvmfs2 $stage_dir/bin/cvmfs_fsck $stage_dir/bin/cvmfs_talk \
           $stage_dir/sbin/mount_cvmfs $(find $stage_dir/lib -type f); do
    for dep in $(otool -L $f | awk 'NR > 1 {print $1}' | grep -v '^/usr/lib/\|^/System/'); do
      case "$dep" in $PREFIX/lib/*) [ -e $stage_dir/${dep#$PREFIX/} ] && continue ;; esac
      src=$(resolve $dep)
      [ -n "$src" ] && [ -e "$src" ] || die "cannot find $dep (from $f)"
      dst=$(target_path $src)
      if [ ! -e $stage_dir/${dst#$PREFIX/} ]; then
        cp -L $src $stage_dir/${dst#$PREFIX/}
        chmod u+w $stage_dir/${dst#$PREFIX/}
        changed=1
      fi
      install_name_tool -change $dep $dst $f 2>/dev/null
    done
  done
done
for f in $stage_dir/bin/cvmfs2 $stage_dir/bin/cvmfs_fsck $stage_dir/bin/cvmfs_talk \
         $stage_dir/sbin/mount_cvmfs $(find $stage_dir/lib -type f); do
  case "$f" in
    *.dylib) install_name_tool -id $(target_path $f) $f 2>/dev/null ;;
  esac
  for rpath in $(otool -l $f | awk '/LC_RPATH/ {getline; getline; print $2}'); do
    install_name_tool -delete_rpath $rpath $f 2>/dev/null
  done
  codesign --force --sign - $f
done
echo "bundled libraries:"
find $stage_dir/lib -type f | sort

DESTDIR=$BUILD_DIR/nfs_pkg_destdir cmake --install $BUILD_DIR/mount
cp -R $BUILD_DIR/nfs_pkg_destdir/$PREFIX/etc $stage_dir/
grep -q '^CVMFS_MOUNT_BACKEND=nfs$' $stage_dir/etc/cvmfs/default.d/11-osx-backend.conf \
  || die "default mount backend is not nfs"

pkgbuild --root $stage_dir \
         --scripts $SOURCE_DIR/packaging/mac/nfs/scripts \
         --identifier ch.cern.cvmfs.pkg.nfs \
         --version "$cvmfs_version" \
         --install-location $PREFIX \
         $result_dir/cvmfs-${cvmfs_version}.pkg
echo "created $result_dir/cvmfs-${cvmfs_version}.pkg"
