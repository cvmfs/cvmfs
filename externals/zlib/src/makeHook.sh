#!/bin/sh

cp Makefile.1211 Makefile
make clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS"
strip -S libz.a
make install
