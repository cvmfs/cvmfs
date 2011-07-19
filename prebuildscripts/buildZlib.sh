#!/bin/sh

env LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" ./configure 
make clean
make
