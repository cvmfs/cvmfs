# CMake module for vendored dependencies
include(FetchContent)

set(LIBARCHIVE_VERSION "3.3.2")
set(LIBARCHIVE_LOCAL_PATH "${CMAKE_SOURCE_DIR}/externals/libarchive/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_URL "https://libarchive.org/downloads/libarchive-3.8.0.tar.gz")
set(LIBARCHIVE_HASH "MD5=d3ed99350b47a53d60ae629160726134")

# -> libarchive <-
# NOTE: This specific version of libarchive exports `archive` and `archive_static` as build targets.
# NOTE: If upgrading to newer version verify target names and make sure they match with usage in cvmfs/CMakeLists.txt

# Configuration
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(ENABLE_ACL OFF)
set(ENABLE_BZip2 OFF)
set(ENABLE_CAT OFF)
set(ENABLE_CNG OFF)
set(ENABLE_CPIO OFF)
set(ENABLE_EXPAT OFF)
set(ENABLE_ICONV OFF)
set(ENABLE_LIBXML2 OFF)
set(ENABLE_LZMA OFF)
set(ENABLE_NETTLE OFF)
set(ENABLE_OPENSSL OFF)
set(ENABLE_TAR OFF)
set(ENABLE_TEST OFF)
set(ENABLE_XATTR OFF)
set(ENABLE_ZLIB OFF)

FetchContent_Declare(
  LibArchive
  URL "${LIBARCHIVE_URL}"
  URL_HASH "${LIBARCHIVE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(LibArchive)
