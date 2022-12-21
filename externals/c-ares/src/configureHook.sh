#!/bin/sh

# Note (OS X): might need XCode CLT to configure c-ares properly
#              Please install XCode and run `xcode-select --install` afterwards.
#              Also remember to start XCode at least once and agree to the EULA.

FIX_COMP=""
if [ x"$(uname)" = x"Darwin" ]; then
  FIX_COMP="CC=/usr/bin/clang CXX=/usr/bin/clang++"
fi

sh configure $FIX_COMP LDFLAGS="$LDFLAGS -rdynamic" \
             CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64" \
             CFLAGS="$CFLAGS $CVMFS_BASE_C_FLAGS -fPIC" \
             --enable-shared=no \
             --enable-tests=no \
             --prefix=$EXTERNALS_INSTALL_LOCATION
