#!/bin/sh

cd build
make clean

make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}

cp -rv googlemock/gtest/*.a $EXTERNALS_INSTALL_LOCATION/lib/

# exiting from build directory
cd ..

cd googletest/include/
cp -rv gtest $EXTERNALS_INSTALL_LOCATION/include/
cd ../..

cd ..
