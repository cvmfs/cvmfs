#!/bin/sh

mkdir build && cd build
CFLAGS="${CVMFS_BASE_C_FLAGS} -fPIC" ../configure \
  --enable-static \
  --disable-shared \
  --disable-tests \
  --prefix=${EXTERNALS_INSTALL_LOCATION}/crypto
