#!/bin/sh

#echo "$@" | grep "enable-libfuse-builtin=no" > /dev/null
#if [ $? -eq 0 ]; then
#  exit 0
#fi
#echo "$@" | grep "enable-libfuse-builtin" > /dev/null
#if [ $? -ne 0 ]; then
#  exit 0
#fi

exec ./configure ${1+"$@"} LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" \
  --enable-lib \
  --disable-util \
  --disable-example

