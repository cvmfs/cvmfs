#!/bin/sh

make clean
make
strip -S lib/.libs/libcurl.a
