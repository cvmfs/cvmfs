#!/bin/sh

cd build
make clean
CFLAGS="$CVMFS_BASE_C_FLAGS -fPIC" CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" make install
strip -S ssl/libssl.a
strip -S crypto/libcrypto.a
make install
