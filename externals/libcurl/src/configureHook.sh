#!/bin/sh

curl_ssl_config="--with-openssl"
FIX_COMP=""
LIBS=""
if [ x"$(uname)" = x"Darwin" ]; then
    curl_ssl_config="--with-openssl=$EXTERNALS_INSTALL_LOCATION"
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
  --enable-symbol-hiding \
  --disable-shared \
  --enable-static \
  --enable-http \
  --enable-http-auth \
  --enable-proxy \
  --enable-file \
  --enable-ipv6 \
  --enable-openssl-auto-load-config \
  --enable-dnsshuffle \
  --disable-ech \
  --disable-ftp \
  --disable-ldap \
  --disable-ldaps \
  --disable-rtsp \
  --disable-dict \
  --disable-telnet \
  --disable-tftp \
  --disable-pop3 \
  --disable-imap \
  --disable-smb \
  --disable-smtp \
  --disable-gopher \
  --disable-mqtt \
  --disable-manual \
  --disable-libcurl-option \
  --disable-threaded-resolver \
  --disable-verbose \
  --disable-sspi \
  --disable-crypto-auth \
  --disable-ntlm \
  --disable-ntlm-wb \
  --disable-tls-srp \
  --disable-unix-sockets \
  --disable-cookies \
  --disable-doh \
  --disable-mime \
  --disable-dateparse \
  --disable-netrc \
  --disable-progress-meter \
  --disable-get-easy-options \
  --disable-alt-svc \
  --disable-headers-api \
  --disable-hsts \
  --disable-websockets \
  ${curl_ssl_config} \
  --without-hyper \
  --without-brotli \
  --without-zstd \
  --without-ca-bundle \
  --without-ca-path \
  --without-libpsl \
  --without-libgsasl \
  --without-librtmp \
  --without-winidn \
  --without-libidn2 \
  --without-nghttp2 \
  --without-ngtcp2 \
  --without-nghttp3 \
  --without-quiche \
  --without-msh3 \
  --without-zsh-functions-dir \
  --without-fish-functions-dir
