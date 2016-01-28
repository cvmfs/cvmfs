#!/bin/sh

# Note (OS X): might need XCode CLT to configure c-ares properly
#              Please install XCode and run `xcode-select --install` afterwards.
#              Also remember to start XCode at least once and agree to the EULA.

sh configure LDFLAGS="$LDFLAGS -rdynamic" CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -fPIC"
