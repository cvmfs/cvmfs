#!/bin/sh

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

autoreconf -i
automake
autoconf
CFLAGS="-fPIC" CXXFLAGS="-fPIC" ./configure --prefix=${SCRIPT_LOCATION}
