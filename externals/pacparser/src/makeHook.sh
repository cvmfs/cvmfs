#!/bin/sh

make -C src
mkdir src/static
cp src/pacparser.o src/libjs.a src/static

cd src/static
ar x libjs.a
rm -f libjs.a
ar rsc libpacparser.a *.o
rm -f *.o
strip -S libpacparser.a 
