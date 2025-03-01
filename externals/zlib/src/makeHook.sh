#!/bin/sh

cp Makefile.1211 Makefile
make clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS" -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
strip -S libz.a
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
