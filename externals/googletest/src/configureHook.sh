#!/bin/sh

mkdir build
cd build

CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -DGTEST_USE_OWN_TR1_TUPLE=1 -fPIC" cmake ..

cd ..
