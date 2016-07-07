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
  # for some reason SLC6 produces an uncompressed *.egg directory containing GeoIP.so
  cp .tmp/*.egg/GeoIP.so .tmp
elif [ -f .tmp/GeoIP.*.so ]; then
  # on ArchLinux no *.egg is created but a GeoIP.<system tag>.so file
  cp .tmp/GeoIP.*.so .tmp/GeoIP.so
fi
cp .tmp/GeoIP.so dist
rm -rf .tmp
