#!/bin/bash
set -e
cd build
make clean
DESTDIR=../install make -j ${CVMFS_BUILD_EXTERNAL_NJOBS} install
cp -rv ../install/usr/local/* "$EXTERNALS_INSTALL_LOCATION"/
