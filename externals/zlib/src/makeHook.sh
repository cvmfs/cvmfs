#!/bin/sh

cp Makefile.128 Makefile
make clean
make
strip -S libz.a
