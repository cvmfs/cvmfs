#!/bin/sh

./configure --disable-dependency-tracking --enable-static LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls"
make clean
make