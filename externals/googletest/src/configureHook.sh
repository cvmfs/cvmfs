#!/bin/sh

mkdir build
cd build

CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" cmake ..

cd ..
