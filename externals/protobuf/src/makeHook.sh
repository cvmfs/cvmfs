#!/bin/sh

make clean
make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
strip -S src/.libs/libprotobuf-lite.a
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
