#!/bin/sh

cd ../../c-ares/src
make clean && make
cd ../../libcurl/src

sh configure LDFLAGS="$LDFLAGS -L${PWD}/../../c-ares/src/.libs -rdynamic" CFLAGS="$CFLAGS -I${PWD}/../../c-ares/src -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" \
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
