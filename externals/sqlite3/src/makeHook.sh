#!/bin/sh

make clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS" -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
strip -S libsqlite3.a

cp -rv *.h $EXTERNALS_INSTALL_LOCATION/include/
cp -rv libsqlite3.a $EXTERNALS_INSTALL_LOCATION/lib/
