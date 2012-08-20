#!/bin/sh

sh configure CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" \
  --disable-dependency-tracking
