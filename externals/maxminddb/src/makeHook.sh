#!/bin/sh

set -e
rm -rf build
python setup.py build
if [ -d build/lib ]; then
  cd build/lib
else
  cd build/lib.*
fi
find *|cpio -pdv $EXTERNALS_INSTALL_LOCATION
