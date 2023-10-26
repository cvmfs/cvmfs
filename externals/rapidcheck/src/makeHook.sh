#!/bin/sh

make clean
make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}

cp -rv include/* $EXTERNALS_INSTALL_LOCATION/include/
cp -rv extras/gtest/include/* $EXTERNALS_INSTALL_LOCATION/include/
cp -rv librapidcheck.a $EXTERNALS_INSTALL_LOCATION/lib/
