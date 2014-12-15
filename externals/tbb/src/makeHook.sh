#!/bin/sh

make clean
export CUSTOM_SUFFIX=_cvmfs;          \
export CXXFLAGS="$CXXFLAGS -Wformat"; \
make
