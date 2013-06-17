#!/bin/sh

cdir=$(pwd)
cares_location="../build_c-ares"
cd $cares_location
sh makeHook.sh
cd $cdir

sh configure LDFLAGS="$LDFLAGS -L${PWD}/${cares_location}/.libs -rdynamic" CFLAGS="$CFLAGS -I${PWD}/${cares_location} -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden" \
  $CVMFS_ZLIB --enable-warnings \
  --enable-ares \
  --disable-shared \
  --enable-static \
  --disable-ftp \
  --disable-file \
  --disable-ldap \
  --disable-ldaps \
  --disable-rtsp \
  --enable-http \
  --enable-proxy \
  --disable-dict \
  --disable-telnet \
  --disable-tftp \
  --disable-pop3 \
  --disable-imap \
  --disable-smtp \
  --disable-gopher \
  --disable-threaded-resolver \
  --disable-manual \
  --enable-ipv6 \
  --disable-sspi \
  --disable-crypto-auth \
  --disable-cookies \
  --enable-hidden-symbols \
  --without-ssl \
  --without-gnutls \
  --without-polarssl \
  --without-nss \
  --without-ca-bundle \
  --without-ca-path \
  --without-libssh2 \
  --without-libidn \
  --without-librtmp \
  --disable-verbose
