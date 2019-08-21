#!/bin/sh

# Not yet clear if we want google benchmark to add as another fixed dependency
# to cvmfs.  It is only used during development, so we might as well pull it
# in as necessary
if [ ! -f CMakeLists.txt ]; then
  git init
  git remote add origin https://github.com/google/benchmark.git
  git fetch
  git checkout 090faecb454fbd6e6e17a75ef8146acb037118d4
fi

CVMFS_PATCHED_CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fexceptions"

CXXFLAGS="$CXXFLAGS $CVMFS_PATCHED_CXXFLAGS" cmake -DCMAKE_BUILD_TYPE=Release \
  -DBENCHMARK_ENABLE_GTEST_TESTS=off \
  -DCMAKE_INSTALL_PREFIX=$EXTERNALS_INSTALL_LOCATION .

