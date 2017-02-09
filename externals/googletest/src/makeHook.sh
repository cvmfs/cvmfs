#!/bin/sh

make clean
make

cp -rv include/gtest $EXTERNALS_INSTALL_LOCATION/include/
cp -rv lib*.a $EXTERNALS_INSTALL_LOCATION/lib/
