#!/bin/sh

set -e

static_result_dir=src/static

FIX_COMP=""
if [ x"$(uname)" = x"Darwin" ]; then
  FIX_COMP="CC=/usr/bin/clang CXX=/usr/bin/clang++"
fi

FIX_PYTHON="PYTHON=python3"
if ! python3 -V >/dev/null 2>&1; then
  FIX_PYTHON="PYTHON=python"
fi

echo "make clean && make for libpacparser (omitting test execution)..."
[ -d $static_result_dir ] && rm -fR $static_result_dir
make $FIX_PYTHON -C src clean -j ${CVMFS_BUILD_EXTERNAL_NJOBS}
make $FIX_PYTHON $FIX_COMP CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS" -j1 -C src pacparser.o spidermonkey/libjs.a  # default target runs tests!
echo "finished internal build of libpacparser"

echo "creating static link library for libpacparser..."
mkdir src/static
cp src/pacparser.o src/spidermonkey/libjs.a $static_result_dir

cd $static_result_dir
ar x libjs.a
rm -f libjs.a
ar rsc libpacparser.a *.o
rm -f *.o
echo "finished creating static link library for libpacparser"


if [ ! -e /etc/fedora-release ] # workaround for a rpmbuild error on fc38
then 
  echo "stripping debug symbols for libpacparser..."
  strip -S libpacparser.a
fi
echo "finished building libpacparser.a"

# Install
cd ../../
cp -v src/*.h $EXTERNALS_INSTALL_LOCATION/include/
cp -v $static_result_dir/libpacparser.a $EXTERNALS_INSTALL_LOCATION/lib/
