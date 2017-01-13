#!/bin/sh

make clean
make CVMFS_EXTERNAL_CXX_FLAGS="$CVMFS_EXTERNAL_CXX_FLAGS"
strip -S libvjson.a

