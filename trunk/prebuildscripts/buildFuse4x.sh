#!/bin/sh

autoreconf -f -i -Wall,no-obsolete
./configure --disable-dependency-tracking --enable-static
make clean
make