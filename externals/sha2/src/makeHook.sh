#!/bin/sh

make clean
make CVMFS_EXTERNAL_C_FLAGS="$CVMFS_EXTERNAL_C_FLAGS"
strip -S libsha2.a

