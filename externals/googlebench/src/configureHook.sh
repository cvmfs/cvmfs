#!/bin/sh

# Not yet clear if we want google benchmark to add as another fixed dependency
# to cvmfs.  It is only used during development, so we might as well pull it
# in as necessary
if [ ! -f CMakeLists.txt ]; then
  git clone https://github.com/google/benchmark.git .
  git checkout 2149577f892116d4080d16fbf0b0455b1026b219
fi

CVMFS_PATCHED_CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fexceptions"

CXXFLAGS="$CXXFLAGS $CVMFS_PATCHED_CXXFLAGS" cmake -DCMAKE_BUILD_TYPE=Release .

