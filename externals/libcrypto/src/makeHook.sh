#!/bin/sh

cd build
make clean
make -C crypto install
make -C include install
