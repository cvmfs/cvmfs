#!/bin/sh

# There is a bug in TBB that gets triggered by GCC6 optimizations:
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=71388
# We set workaround CXXFLAGS if necessary
if echo "int main() {}" | c++ -fno-lifetime-dse -E - >/dev/null 2>&1; then
  TBB_WORKAROUND_CXX_FLAGS="-fno-lifetime-dse"
fi


make clean
export CUSTOM_SUFFIX=_cvmfs;                                     \
export CXXFLAGS="$CXXFLAGS -Wformat $TBB_WORKAROUND_CXX_FLAGS";  \
make
