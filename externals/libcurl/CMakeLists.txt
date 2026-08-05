# CMake module for vendoring libcurl
include(FetchContent)

set(CURL_VERSION "7.86.0")
set(CURL_URL "https://curl.se/download/curl-${CURL_VERSION}.tar.bz2")
set(CURL_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/curl-${CURL_VERSION}.tar.bz2")
set(CURL_HASH "MD5=9ce4ebba8a446e7c176dd35899004d33")

# -> libcurl <-
# NOTE: libcurl exports the target `CURL::libcurl`. The options below mirror the
# NOTE: former autotools configure flags (externals/libcurl/src/configureHook.sh).
# NOTE: The old reenable_poll_darwin.patch only patched the autotools `configure`
# NOTE: script; the CMake build has its own poll detection, so it is not needed.

set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME external_deps)

# Static, position-independent library, no curl executable or tests.
set(BUILD_SHARED_LIBS OFF)
set(BUILD_CURL_EXE    OFF)
set(BUILD_TESTING     OFF)
set(ENABLE_MANUAL     OFF)

# Don't generate/install curl's CMake package (CURLTargets export). The externals
# framework consumes the raw libcurl.a + headers from externals_install, not
# curl's find_package config. Keeping the export on also breaks generation: with
# ENABLE_ARES, libcurl's link interface references the in-build c-ares target,
# which is not part of any export set, so install(EXPORT CURLTargets) errors out.
set(CURL_ENABLE_EXPORT_TARGET OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -fPIC")

# Async DNS via c-ares (built by externals/c-ares); TLS via OpenSSL.
set(ENABLE_ARES       ON)
set(CURL_USE_OPENSSL  ON)

# Hide private symbols (was --enable-symbol-hiding).
set(CURL_HIDDEN_SYMBOLS ON)
set(ENABLE_IPV6         ON)

# Drop dependencies we do not want linked in (was the --without-* flags).
set(CURL_USE_LIBPSL   OFF)
set(CURL_USE_LIBSSH2  OFF)
set(CURL_USE_LIBIDN2  OFF)
set(CURL_BROTLI       OFF)
set(CURL_ZSTD         OFF)
set(USE_NGHTTP2       OFF)

# No baked-in CA bundle/path (was --without-ca-bundle --without-ca-path).
set(CURL_CA_BUNDLE   "none" CACHE STRING "" FORCE)
set(CURL_CA_PATH     "none" CACHE STRING "" FORCE)
set(CURL_CA_FALLBACK OFF)

# Keep only the protocols/features cvmfs needs: HTTP(S), FILE, PROXY.
# Everything below mirrors the --disable-* flags of the autotools build.
set(CURL_DISABLE_FTP          ON)
set(CURL_DISABLE_LDAP         ON)
set(CURL_DISABLE_LDAPS        ON)
set(CURL_DISABLE_RTSP         ON)
set(CURL_DISABLE_DICT         ON)
set(CURL_DISABLE_TELNET       ON)
set(CURL_DISABLE_TFTP         ON)
set(CURL_DISABLE_POP3         ON)
set(CURL_DISABLE_IMAP         ON)
set(CURL_DISABLE_SMB          ON)
set(CURL_DISABLE_SMTP         ON)
set(CURL_DISABLE_GOPHER       ON)
set(CURL_DISABLE_MQTT         ON)
set(CURL_DISABLE_DOH          ON)
set(CURL_DISABLE_COOKIES      ON)
set(CURL_DISABLE_NETRC        ON)
set(CURL_DISABLE_ALTSVC       ON)
set(CURL_DISABLE_HSTS         ON)
set(CURL_DISABLE_CRYPTO_AUTH  ON)
set(CURL_DISABLE_NTLM         ON)
set(CURL_DISABLE_VERBOSE_STRINGS ON)
set(ENABLE_UNIX_SOCKETS       OFF)

# macOS: the libcurl build needs OpenSSL headers. On Linux the system OpenSSL is
# picked up automatically; on macOS point CMake at a Homebrew/OpenSSL install if
# OPENSSL_ROOT_DIR was not already provided. c-ares >= 1.16 pulls in libresolv,
# which curl links transitively through the c-ares::cares target.
if (APPLE AND NOT DEFINED OPENSSL_ROOT_DIR)
  if (EXISTS "/opt/homebrew/opt/openssl@3")
    set(OPENSSL_ROOT_DIR "/opt/homebrew/opt/openssl@3")
  elseif (EXISTS "/usr/local/opt/openssl@3")
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl@3")
  endif()
endif()

setup_external_build_mode(CURL)

FetchContent_Declare(
  CURL
  URL "${CURL_URL}" "${CURL_URL_MIRROR}"
  URL_HASH "${CURL_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

FetchContent_MakeAvailable(CURL)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(CURL_FOUND)
  message(STATUS "Found libcurl target")
  set_target_properties(CURL::libcurl PROPERTIES IMPORTED_GLOBAL TRUE)
else()
  message(STATUS "Building libcurl from vendored sources.")
  install(TARGETS libcurl DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(DIRECTORY ${curl_SOURCE_DIR}/include/curl DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL FILES_MATCHING PATTERN "*.h")
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "libcurl" PARENT_SCOPE)
endif()
