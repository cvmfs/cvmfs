#!/bin/sh

sh configure CFLAGS="$(echo $CVMFS_BASE_CXX_FLAGS | sed s/-fvisibility=hidden//) -fPIC" \
    --with-pic \
    --disable-shared \
    --prefix=$EXTERNALS_INSTALL_LOCATION

