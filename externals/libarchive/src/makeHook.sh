#!/bin/sh

if [[ "x$(uname -s)" == "xDarwin" ]]; then
  cd mybuild
fi

make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}

make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
