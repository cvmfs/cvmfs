#!/usr/bin/env bash
set -eu

if [[ "$(uname -s)" == "Darwin" ]]; then
  cd mybuild
fi

make -j ${CVMFS_BUILD_EXTERNAL_NJOBS}

make install -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
