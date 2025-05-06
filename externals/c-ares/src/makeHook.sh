#!/bin/sh

make clean
make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
# Don't strip debug symbols
# strip -S .libs/libcares.a
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
