#!/bin/sh

cd build
make clean
make -C crypto install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
make -C include install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
