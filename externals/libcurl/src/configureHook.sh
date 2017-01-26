#!/bin/sh

curl_ssl_config="--with-ssl"
if [ x"$(uname)" = x"Darwin" ]; then
    curl_ssl_config="--with-ssl=$EXTERNALS_INSTALL_LOCATION"
fi

sh configure CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64" \
  LDFLAGS="$LDFLAGS -rdynamic" \
  CFLAGS="$CFLAGS $CVMFS_BASE_C_FLAGS -fvisibility=hidden -fPIC" \
  $CVMFS_ZLIB --enable-warnings \
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
  --without-winssl \
  --without-darwinssl \
  --without-gnutls \
  --without-polarssl \
  --without-mbedtls \
  --without-cyassl \
  --without-axtls \
  --without-nss \
  --without-ca-bundle \
  --without-ca-path \
  --without-libpsl \
  --without-libssh2 \
  --without-libmetalink \
  --without-libidn \
  --without-libidn2 \
  --without-winidn \
  --without-librtmp \
  --without-nghttp2 \
  --without-zsh-functions-dir
