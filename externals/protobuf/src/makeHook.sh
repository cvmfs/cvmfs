#!/bin/sh

make clean
make
strip -S src/.libs/libprotobuf-lite.a
