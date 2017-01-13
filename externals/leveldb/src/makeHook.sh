#!/bin/sh

make clean
CFLAGS="$CVMFS_EXTERNAL_C_FLAGS -fPIC" CXXFLAGS="$CVMFS_EXTERNAL_CXX_FLAGS -fPIC" make
strip -S libleveldb.a
