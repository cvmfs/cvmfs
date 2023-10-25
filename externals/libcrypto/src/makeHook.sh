#!/bin/sh

cd build
make clean
make -C crypto install -j
make -C include install -j
