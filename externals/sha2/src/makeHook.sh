#!/bin/sh

make clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS"
strip -S libsha2.a

