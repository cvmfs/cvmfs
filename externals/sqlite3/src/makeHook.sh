#!/bin/sh

make clean
make
strip -S libsqlite3.a
