#!/bin/sh

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

