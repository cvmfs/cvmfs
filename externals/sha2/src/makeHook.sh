#!/bin/sh

make clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS"
strip -S libsha2.a

cp -rv *.h $EXTERNALS_INSTALL_LOCATION/include/
cp -rv libsha2.a $EXTERNALS_INSTALL_LOCATION/lib/