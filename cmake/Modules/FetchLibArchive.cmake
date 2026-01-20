# CMake module for vendored dependencies
include(FetchContent)

set(LIBARCHIVE_VERSION "3.8.0")
set(LIBARCHIVE_LOCAL_PATH "${CMAKE_SOURCE_DIR}/externals/libarchive/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_URL "https://libarchive.org/downloads/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_HASH "MD5=d3ed99350b47a53d60ae629160726134")

function(get_source_url NAME LOCAL_PATH REMOTE_URL RESULT_VAR)
  if(EXISTS "${LOCAL_PATH}")
    message(STATUS "${NAME}: Using local archive: ${LOCAL_PATH}")
    set(${RESULT_VAR} "${LOCAL_PATH}" PARENT_SCOPE)
  elseif(NOT "${REMOTE_URL}" STREQUAL "")
    message(STATUS "${NAME}: Using remote URL: ${REMOTE_URL}")
    set(${RESULT_VAR} "${REMOTE_URL}" PARENT_SCOPE)
  else()
    message(FATAL_ERROR "${NAME}: Local archive not found and no remote URL provided!")
  endif()
endfunction()

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

get_source_url("libarchive" "${LIBARCHIVE_LOCAL_PATH}" "${LIBARCHIVE_URL}" LIBARCHIVE_SRC)
FetchContent_Declare(
  LibArchive
  URL "${LIBARCHIVE_SRC}"
  URL_HASH "${LIBARCHIVE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(LibArchive)
