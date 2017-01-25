#!/bin/sh

make clean
make
#rm -f libGeoIP/.libs/*.so*
make install
