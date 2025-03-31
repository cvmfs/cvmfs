#!/bin/sh

sh configure CFLAGS="$CFLAGS $CVMFS_BASE_C_FLAGS -D_FILE_OFFSET_BITS=64" \
             CXXFLAGS="$CXXFLAGS $CVMFS_BASE_CXX_FLAGS -D_FILE_OFFSET_BITS=64" \
             --disable-dependency-tracking \
             --prefix=$EXTERNALS_INSTALL_LOCATION
