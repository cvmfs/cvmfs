#!/bin/sh

set -e
LIBGEOIP="$PWD/../build_libgeoip/libGeoIP"
rm -rf build
CFLAGS="-I$LIBGEOIP" LDFLAGS="-L$LIBGEOIP/.libs" python setup.py build
mkdir -p .tmp
PYTHONPATH=$PWD/.tmp python setup.py install --install-lib=$PWD/.tmp
rm -rf dist
mkdir dist
if [ -f .tmp/*.egg ]; then
  # older python versions like 2.4 install a .egg; extract GeoIP.so
  (cd .tmp; unzip -o *.egg GeoIP.so)
elif [ -d .tmp/*.egg ]; then
  # the .so might be in an uncompressed *.egg directory
  # on arch linux the shared object is called GeoIP.<system tag>.so
  cp .tmp/*.egg/GeoIP*.so .tmp
fi
cp .tmp/GeoIP*.so dist/GeoIP.so
rm -rf .tmp
