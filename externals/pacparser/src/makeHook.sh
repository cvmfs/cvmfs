#!/bin/sh

set -e

static_result_dir=src/static

[ -d $static_result_dir ] && rm -fR $static_result_dir
make -C src clean
make -C src

mkdir src/static
cp src/pacparser.o src/libjs.a $static_result_dir

cd $static_result_dir
ar x libjs.a
rm -f libjs.a
ar rsc libpacparser.a *.o
rm -f *.o
strip -S libpacparser.a
