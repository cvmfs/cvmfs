#!/bin/sh

CXXFLAGS="$CVMFS_EXTERNAL_CXX_FLAGS -fPIC" ./configure --without-zlib --disable-shared
