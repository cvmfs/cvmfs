#!/bin/sh

sh configure LDFLAGS="$LDFLAGS -rdynamic" CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -fPIC"
  
