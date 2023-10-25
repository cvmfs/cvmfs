#!/bin/sh

make clean
make -j
# Don't strip debug symbols
# strip -S .libs/libcares.a
make install -j
