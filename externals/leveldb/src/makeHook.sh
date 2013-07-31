#!/bin/sh

make clean
CXXFLAGS=-fPIC CFLAGS=-fPIC make
strip -S libleveldb.a
