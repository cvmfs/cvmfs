#!/bin/sh

cdir=$(pwd)
ssl_install_dir=${cdir}/ssl_install
mkdir build && cd build
cmake -D CMAKE_INSTALL_PREFIX=$EXTERNALS_INSTALL_LOCATION -DCMAKE_C_FLAGS="$CVMFS_BASE_C_FLAGS" ../
