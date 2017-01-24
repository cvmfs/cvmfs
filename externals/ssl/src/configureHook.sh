#!/bin/sh

cdir=$(pwd)
ssl_install_dir=${cdir}/ssl_install
mkdir build && cd build
cmake -D CMAKE_INSTALL_PREFIX=$ssl_install_dir ../
