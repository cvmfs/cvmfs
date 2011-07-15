#!/bin/sh

echo "$@" | grep "enable-zlib-builtin=no" > /dev/null
if [ $? -eq 0 ]; then
  exit 0
fi
echo "$@" | grep "enable-zlib-builtin" > /dev/null
if [ $? -ne 0 ]; then
  exit 0
fi

env LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" ./configure 

