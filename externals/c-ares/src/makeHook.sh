#!/bin/sh

make clean
make
strip -S .libs/libcares.a

