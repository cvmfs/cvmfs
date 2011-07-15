#!/bin/sh

echo "$@" | grep "enable-libcurl-builtin=no" > /dev/null
if [ $? -eq 0 ]; then
  exit 0
fi
echo "$@" | grep "enable-libcurl-builtin" > /dev/null
if [ $? -ne 0 ]; then
  exit 0
fi

exec ./configure ${1+"$@"} LDFLAGS="$LDFLAGS -rdynamic" CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls" \
  $CVMFS_ZLIB --enable-warnings \
  --disable-ares \
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
  --disable-ipv6 \
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
  --without-libidn

