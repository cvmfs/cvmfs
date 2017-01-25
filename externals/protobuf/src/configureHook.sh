#!/bin/sh

CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" ./configure --without-zlib --disable-shared \
    --prefix=$EXTERNALS_INSTALL_LOCATION
