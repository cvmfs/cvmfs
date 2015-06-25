#!/bin/sh

make clean
make
strip -S libsha2.a

