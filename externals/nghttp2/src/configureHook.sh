#!/bin/sh

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

autoreconf -i
automake
autoconf
./configure --prefix=${SCRIPT_LOCATION} \
            --with-pic=yes              \
            --enable-lib-only
