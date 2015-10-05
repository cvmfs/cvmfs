#!/bin/sh

make clean
make
strip -S libvjson.a

