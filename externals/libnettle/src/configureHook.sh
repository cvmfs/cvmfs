#!/bin/sh

mkdir build && cd build


CFLAGS="${CVMFS_BASE_C_FLAGS} -fPIC" ../configure \
  --disable-shared \
  --disable-openssl \
  --disable-documentation \
  --enable-arm64-crypto \
  --enable-x86-aesni \
  --enable-x86-sha-ni \
  --enable-x86-pclmul \
  --enable-mini-gmp \
  --prefix=${EXTERNALS_INSTALL_LOCATION}
