#!/bin/sh

make clean
make libcares.la
strip -S .libs/libcares.a

