#!/bin/sh

set -e
rm -rf build
python setup.py build
cd build
find lib|cpio -pdv $EXTERNALS_INSTALL_LOCATION
