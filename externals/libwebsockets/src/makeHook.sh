#!/bin/sh

make -C build

cp -rv build/include/libwebsockets.h \
       build/include/lws_config.h \
       $EXTERNALS_INSTALL_LOCATION/include/
cp -rv build/lib/libwebsockets.a $EXTERNALS_INSTALL_LOCATION/lib/
