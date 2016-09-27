#!/bin/sh

make clean
make
# Don't strip debug dymbols
# strip -S lib/.libs/libcurl.a
