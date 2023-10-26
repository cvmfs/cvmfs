#!/bin/sh

make clean
make CVMFS_BASE_CXX_FLAGS="$CVMFS_BASE_CXX_FLAGS" -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
strip -S libvjson.a

cp -rv json.h block_allocator.h $EXTERNALS_INSTALL_LOCATION/include/
cp -rv libvjson.a $EXTERNALS_INSTALL_LOCATION/lib/
