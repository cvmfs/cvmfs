#!/bin/sh

make clean
make
strip -S libsha3.a

