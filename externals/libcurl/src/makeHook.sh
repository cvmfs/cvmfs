#!/bin/sh

make clean
make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
# Don't strip debug dymbols
# strip -S lib/.libs/libcurl.a
make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
