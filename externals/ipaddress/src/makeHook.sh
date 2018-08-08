#!/bin/sh

set -e
rm -rf build
python setup.py build
cd build/lib*
find *|cpio -pdv $EXTERNALS_INSTALL_LOCATION
