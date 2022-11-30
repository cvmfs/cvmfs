#!/bin/sh

cd build
make clean

make

cp -rv googlemock/gtest/*.a $EXTERNALS_INSTALL_LOCATION/lib/
cp -rv googlemock/*.a $EXTERNALS_INSTALL_LOCATION/lib/

# exiting from build directory
cd ..

# enter into the directory of gmock
cd googlemock/include/
# move the headers files
cp -rv gmock $EXTERNALS_INSTALL_LOCATION/include/
# reset location
cd ../..

# repeat
cd googletest/include/
cp -rv gtest $EXTERNALS_INSTALL_LOCATION/include/
cd ../..

cd ..
