#!/bin/sh

autoreconf -vfi
CFLAGS="$CVMFS_BASE_C_FLAGS -fPIC" CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" ./configure --without-zlib --disable-shared \
    --prefix=$EXTERNALS_INSTALL_LOCATION
