#!/bin/sh

make clean
make 
touch zlib.pc
make install
