#!/bin/sh

make clean
make CVMFS_BASE_CXX_FLAGS="$CVMFS_BASE_CXX_FLAGS"
strip -S libvjson.a

