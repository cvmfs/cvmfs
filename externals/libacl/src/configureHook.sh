#!/bin/sh

CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC -g" ./configure --prefix=$EXTERNALS_INSTALL_LOCATION --enable-shared=no
