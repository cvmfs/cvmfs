#!/bin/sh

cdir=$(pwd)
cares_location="../build_c-ares"
cd $cares_location
sh makeHook.sh
cd $cdir

sh configure CPPFLAGS="$CPPFLAGS -I${PWD}/${cares_location} -D_FILE_OFFSET_BITS=64" LDFLAGS="$LDFLAGS -L${PWD}/${cares_location}/.libs -rdynamic" CFLAGS="$CFLAGS -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -fPIC" \
  $CVMFS_ZLIB --enable-warnings \
  --enable-ares \
  --disable-shared \
  --enable-static \
  --disable-ftp \
  --enable-file \
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
  --enable-symbol-hiding \
  --disable-tls-srp \
  --disable-ntlm-wb \
  --with-ssl \
  --without-winssl \
  --without-darwinssl \
  --without-gnutls \
  --without-polarssl \
  --without-cyassl \
  --without-axtls \
  --without-nss \
  --without-ca-bundle \
  --without-ca-path \
  --without-libssh2 \
  --without-libmetalink \
  --without-libidn \
  --without-winidn \
  --without-librtmp \
  --without-nghttp2 \
  --disable-verbose
