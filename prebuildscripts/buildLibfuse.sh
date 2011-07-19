#!/bin/sh

sh ./configure LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" \
  --enable-lib \
  --disable-util \
  --disable-example

make clean
make