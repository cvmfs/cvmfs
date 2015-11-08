#!/bin/sh

set -e

static_result_dir=src/static

echo "make clean && make for libpacparser (omitting test execution)..."
[ -d $static_result_dir ] && rm -fR $static_result_dir
make -C src clean
make -j1 -C src pacparser.o libjs.a # default target runs tests!
echo "finished internal build of libpacparser"

echo "creating static link library for libpacparser..."
mkdir src/static
cp src/pacparser.o src/libjs.a $static_result_dir

cd $static_result_dir
ar x libjs.a
rm -f libjs.a
ar rsc libpacparser.a *.o
rm -f *.o
echo "finished creating static link library for libpacparser"

echo "stripping debug symbols for libpacparser..."
strip -S libpacparser.a
echo "finished building libpacparser.a"
