#!/bin/sh

cp Makefile.128 Makefile
make clean
make CVMFS_EXTERNAL_C_FLAGS="$CVMFS_EXTERNAL_C_FLAGS"
strip -S libz.a
