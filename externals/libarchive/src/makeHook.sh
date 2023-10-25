#!/bin/sh

if [[ "x$(uname -s)" == "xDarwin" ]]; then
  cd mybuild
fi

make -j

make install -j
