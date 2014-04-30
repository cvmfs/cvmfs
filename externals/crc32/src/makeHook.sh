#!/bin/sh

make clean
make
strip -S libcrc32.a
