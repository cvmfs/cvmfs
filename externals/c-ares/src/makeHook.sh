#!/bin/sh

make clean
make
# Don't strip debug symbols
# strip -S .libs/libcares.a

