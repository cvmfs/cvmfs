#!/bin/sh

make clean
make
strip -S lib/.libs/libnghttp2.a
