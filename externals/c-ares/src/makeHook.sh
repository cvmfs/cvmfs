#!/bin/sh

make clean
make libcares.la
# Don't strip debug symbols
# strip -S .libs/libcares.a

