#!/bin/sh

cd build
make clean
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
