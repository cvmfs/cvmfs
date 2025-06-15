#!/bin/sh

cdir=$(pwd)
libarchive_install_dir=${cdir}/libarchive_install


# the configure script does not work on macos13,
# so use cmake only in that case, as we don't want 
# to introduce an additional cmake dependency

# CMAKE_POLICY_VERSION_MINIMUM is a workaround to fix compilation with cmake 4

if [ "$(uname -s)" == "Darwin" ]; then
mkdir mybuild && cd mybuild
cmake \
  -DCMAKE_INSTALL_PREFIX=$EXTERNALS_INSTALL_LOCATION \
  -DCMAKE_C_FLAGS="$CVMFS_BASE_C_FLAGS -fPIC" \
  -DENABLE_ACL=OFF \
  -DENABLE_BZip2=OFF \
  -DENABLE_CAT=OFF \
  -DENABLE_CAT_SHARED=OFF \
  -DENABLE_CNG=OFF \
  -DENABLE_COVERAGE=OFF \
  -DENABLE_CPIO=OFF \
  -DENABLE_CPIO_SHARED=OFF \
  -DENABLE_EXPAT=OFF \
  -DENABLE_ICONV=OFF \
  -DENABLE_INSTALL=ON \
  -DENABLE_LIBXML2=OFF \
  -DENABLE_LZMA=OFF \
  -DENABLE_LZO=OFF \
  -DENABLE_LibGCC=ON \
  -DENABLE_NETTLE=OFF \
  -DENABLE_OPENSSL=OFF \
  -DENABLE_TAR=OFF \
  -DENABLE_TAR_SHARED=OFF \
  -DENABLE_TEST=OFF \
  -DENABLE_XATTR=OFF \
  -DENABLE_ZLIB=OFF \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  ../

else
  ./configure CFLAGS="$CFLAGS $CVMFS_BASE_C_FLAGS -fPIC" \
    --enable-static \
    --disable-shared \
    --disable-bsdtar \
    --disable-bsdcat \
    --disable-bsdcpio \
    --without-zlib \
    --without-bz2lib \
    --without-lz4 \
    --without-lzma \
    --without-lzo2 \
    --without-cng \
    --without-nettle \
    --without-openssl \
    --without-xml2 \
    --without-expat \
    --without-iconv \
    --prefix=$EXTERNALS_INSTALL_LOCATION/

  touch libarchive/test/test_foo.c tar/test/test_foo.c cpio/test/test_foo.c cat/test/test_foo.c
  touch libarchive/test/list.h tar/test/list.h cpio/test/list.h cat/test/list.h
fi
