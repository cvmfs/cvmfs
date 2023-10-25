#!/bin/sh

make clean
make -j
strip -S src/.libs/libprotobuf-lite.a
make install -j
