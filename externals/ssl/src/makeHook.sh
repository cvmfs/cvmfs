#!/bin/sh

cd build
make clean
CFLAGS="$CVMFS_BASE_C_FLAGS -fPIC" CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" make install
strip -S ../ssl_install/lib/libssl.a
strip -S ../ssl_install/lib/libcrypto.a
