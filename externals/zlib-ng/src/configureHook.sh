#!/bin/sh
echo "zlib-ng: Configuring in $PWD"
echo "zlib-ng: Configuring with ./configure --static --prefix=$EXTERNALS_INSTALL_LOCATION --zlib-compat"
CFLAGS="-fPIC" ./configure --static --prefix=$EXTERNALS_INSTALL_LOCATION --zlib-compat  

