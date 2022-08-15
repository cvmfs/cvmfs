#!/bin/sh

# We install libcrypto into ${EXTERNALS_INSTALL_LOCATION}/crypto in order
# to not taint the standard include / library paths with it. The LibreSSL
# libcrypto is only supposed to be picked up by libcvmfs_crypto

mkdir build && cd build
CFLAGS="${CVMFS_BASE_C_FLAGS} -fPIC" ../configure \
  --enable-static \
  --disable-shared \
  --disable-tests \
  --prefix=${EXTERNALS_INSTALL_LOCATION}/crypto
