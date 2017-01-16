#!/bin/sh

cdir=$(pwd)
libressl_install_dir=${cdir}/libressl_install
mkdir build && cd build
cmake -D CMAKE_INSTALL_PREFIX=$libressl_install_dir ../