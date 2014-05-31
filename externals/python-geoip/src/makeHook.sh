#!/bin/sh

set -e
LIBGEOIP="$PWD/../build_libgeoip/libGeoIP"
rm -rf build dist
CFLAGS="-I$LIBGEOIP" LDFLAGS="-L$LIBGEOIP/.libs" python setup.py build
mkdir -p .tmp
PYTHONPATH=$PWD/.tmp python setup.py install --install-lib=$PWD/.tmp
rm -rf .tmp
cd dist
unzip -o *.egg GeoIP.so GeoIP.py
rm *.egg
