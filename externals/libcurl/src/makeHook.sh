#!/bin/sh

make clean
make -j
# Don't strip debug dymbols
# strip -S lib/.libs/libcurl.a
make install -j
