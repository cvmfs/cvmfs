#!/bin/sh

if [[ "x$(uname -s)" == "xDarwin" ]]; then
  cd mybuild
fi

make

make install
