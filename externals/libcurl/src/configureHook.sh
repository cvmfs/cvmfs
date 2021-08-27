#!/bin/sh

curl_ssl_config="--with-ssl"
FIX_COMP=""
LIBS=""
if [ x"$(uname)" = x"Darwin" ]; then
    curl_ssl_config="--with-ssl=$EXTERNALS_INSTALL_LOCATION"
  FIX_COMP="CC=/usr/bin/clang CXX=/usr/bin/clang++"
  # On macOS, c-ares >= 1.16.1 uses libresolv for finding name servers
  LIBS="-lresolv"
fi

sh configure $FIX_COMP CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64" \
  LDFLAGS="$LDFLAGS -rdynamic" \
  LIBS="$LIBS" \
  CFLAGS="$CFLAGS $CVMFS_BASE_C_FLAGS -fvisibility=hidden -fPIC" \
  --enable-warnings \
  --prefix=$EXTERNALS_INSTALL_LOCATION \
  --enable-ares=$EXTERNALS_INSTALL_LOCATION \
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
  --disable-smb \
  --disable-smtp \
  --disable-gopher \
  --disable-threaded-resolver \
  --disable-manual \
  --enable-ipv6 \
  --disable-verbose \
  --disable-sspi \
  --disable-crypto-auth \
  --disable-cookies \
  --enable-symbol-hiding \
  --disable-tls-srp \
  --disable-ntlm-wb \
  --disable-unix-sockets \
  ${curl_ssl_config} \
  --without-brotli \
  --without-winssl \
  --without-darwinssl \
  --without-gnutls \
  --without-polarssl \
  --without-mbedtls \
  --without-wolfssl \
  --without-cyassl \
  --without-mesalink \
  --without-nss \
  --without-ca-bundle \
  --without-ca-path \
  --without-libpsl \
  --without-libssh2 \
  --without-libmetalink \
  --without-libidn2 \
  --without-winidn \
  --without-librtmp \
  --without-nghttp2 \
  --without-zsh-functions-dir
