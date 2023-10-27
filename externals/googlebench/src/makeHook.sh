#!/bin/sh

make clean
make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
