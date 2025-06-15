#!/bin/sh

cdir=$(pwd)
ssl_install_dir=${cdir}/ssl_install

# needed for cmake4 on macos
macos_compat_arg=""
if [ "$(uname -s)" == "Darwin" ]; then
  macos_compat_arg=" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 "
fi

mkdir build && cd build
cmake -DLIBRESSL_APPS=off -DLIBRESSL_TESTS=off \
  -DCMAKE_INSTALL_PREFIX=$EXTERNALS_INSTALL_LOCATION -DCMAKE_C_FLAGS="$CVMFS_BASE_C_FLAGS" ${macos_compat_arg} ../
